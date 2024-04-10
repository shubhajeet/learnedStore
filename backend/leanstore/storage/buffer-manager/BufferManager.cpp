#include "BufferManager.hpp"
#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/convert.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <functiontimer.hpp>
#include <iomanip>
#include <leanstore/storage/btree/core/BTreeNode.hpp>
#include <set>
// -------------------------------------------------------------------------------------
// Local GFlags
// -------------------------------------------------------------------------------------
using std::thread;
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd) : ssd_fd(ssd_fd)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
#ifdef TRACK_WITH_HT
      bf_ht.rehash(dram_pool_size * 2);
#else
      bf_vt.resize(2, {nullptr, 0, 0});
      // bf_vt.resize(dram_pool_size, nullptr);
#endif
      INFO("Total Number of Buffer Frames %lu\n", dram_pool_size);
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      // bfs = new BufferFrame[dram_pool_size + safety_pages];

      bfs = reinterpret_cast<BufferFrame*>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      madvise(bfs, dram_total_size,
              MADV_DONTFORK);  // O_DIRECT does not work with forking.
      // // -------------------------------------------------------------------------------------
      // MyNote:: Not sure the role of partition
      // Initialize partitions
      partitions_count = (1 << FLAGS_partition_bits);
      partitions_mask = partitions_count - 1;
      INFO("partition count: %llu", partitions_count);

      const u64 free_bfs_limit = std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      INFO("Free Buffer Frames Limit %lu (Each partition)\n", free_bfs_limit);
      const u64 cooling_bfs_upper_bound = std::ceil((FLAGS_cool_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      INFO("Cooling Buffer Frames Limit %lu (Each partition)\n", cooling_bfs_upper_bound);
      partitions = reinterpret_cast<Partition*>(malloc(sizeof(Partition) * partitions_count));
      for (u64 p_i = 0; p_i < partitions_count; p_i++) {
         new (partitions + p_i) Partition(p_i, partitions_count, free_bfs_limit, cooling_bfs_upper_bound);
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            partitions[p_i].dram_free_list.push(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % partitions_count;
         }
      });
      // -------------------------------------------------------------------------------------
   }
   // std::cout << "page_provider_thread: " << FLAGS_pp_threads << std::endl;
   // -------------------------------------------------------------------------------------
   // Page Provider threads
   if (FLAGS_pp_threads) {  // make it optional for pure in-memory experiments
      std::vector<thread> pp_threads;
      const u64 partitions_per_thread = partitions_count / FLAGS_pp_threads;
      INFO("Partitions for each thread %lu\n", partitions_per_thread);
      ensure(FLAGS_pp_threads <= partitions_count);
      const u64 extra_partitions_for_last_thread = partitions_count % FLAGS_pp_threads;
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         pp_threads.emplace_back(
             [&](u64 t_i, u64 p_begin, u64 p_end) {
                CPUCounters::registerThread("pp_" + std::to_string(t_i));
                // https://linux.die.net/man/2/setpriority
                if (FLAGS_root) {
                   posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
                }
                pageProviderThread(p_begin, p_end);
             },
             t_i, t_i * partitions_per_thread,
             ((t_i + 1) * partitions_per_thread) + ((t_i == FLAGS_pp_threads - 1) ? extra_partitions_for_last_thread : 0));
         bg_threads_counter++;
      }
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         thread& page_provider_thread = pp_threads[t_i];
         cpu_set_t cpuset;
         CPU_ZERO(&cpuset);
         CPU_SET(t_i, &cpuset);
         posix_check(pthread_setaffinity_np(page_provider_thread.native_handle(), sizeof(cpu_set_t), &cpuset) == 0);
         page_provider_thread.detach();
      }
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::clearSSD()
{
   // TODO
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   // MyNote:: write all pages loaded in buffer frame called by leanstore destructor if persist flag is set
   INFO("Writing all the pages in buffer frame to disk");
   stopBackgroundThreads();
   ensure(!FLAGS_out_of_place);
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure(ret == PAGE_SIZE);
         }
         bf.header.latch.mutex.unlock();
      }
   });
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   // TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_freed_pages += partitions[p_i].freedPages();
      total_used_pages += partitions[p_i].allocatedPages();
   }
   return total_used_pages - total_freed_pages;
}

u64 BufferManager::freeDRAM()
{
   u64 total_free_bf = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_free_bf += partitions[p_i].freeDRAM();
   }
   return total_free_bf;
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::getContainingBufferFrame(const u8* ptr)
{
   u64 index = (ptr - reinterpret_cast<u8*>(bfs)) / (sizeof(BufferFrame));
   return bfs[index];
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
Partition& BufferManager::randomPartition()
{
   auto rand_partition_i = utils::RandomGenerator::getRand<u64>(0, partitions_count);
   return partitions[rand_partition_i];
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, dram_pool_size);
   return bfs[rand_buffer_i];
}

/*
BufferFrame* BufferManager::getBufferFrame(PID pid)
{
   auto bf_itr = bf_ht.find(pid);
   if (bf_itr != bf_ht.end()) {
      auto bf = bf_itr->second;
      if (bf->header.state == BufferFrame::STATE::COOL) {
         OptimisticGuard bf_guard(bf->header.latch, true);
         ExclusiveGuard bf_x_guard(bf_guard);  // child
         bf->header.state = BufferFrame::STATE::HOT;
         return bf;
      } else if (bf->header.state == BufferFrame::STATE::HOT) {
         return bf;
      } else {
         return nullptr;
      }
   } else {
      return nullptr;
   }
};
*/

// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   // Pick a pratition randomly
   Partition& partition = randomPartition();
   BufferFrame& free_bf = partition.dram_free_list.pop();
   PID free_pid = partition.nextPID();
   assert(free_bf.header.state == BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.latch.assertNotExclusivelyLatched();
   free_bf.header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::STATE::HOT;
   free_bf.header.lastWrittenGSN = free_bf.page.GSN = 0;
   // -------------------------------------------------------------------------------------
   if (free_pid == dram_pool_size) {
      cout << "----------------------------------------------------------------------------------"
              "---"
           << endl;
      cout << "Going out of memory !" << endl;
      const u64 written_pages = consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Datasize at out of memory: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().allocate_operations_counter++;
   }
   // -------------------------------------------------------------------------------------
   // MyNote:: Allocated new page in buffer frame
   // std::cout << "Allocated pid: " << free_pid  << " bf: " << &(free_bf) << std::endl;
   trackPID(free_pid, &free_bf);
   return free_bf;
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
   Partition& partition = getPartition(bf.header.pid);
   // MyNote:: free page refers to the pid which are not longer used i.e. not a valid page id anymore
   partition.freePage(bf.header.pid);
   // MyNote:: Check what free page does.
   untrackPID(bf.header.pid);

   // -------------------------------------------------------------------------------------
   if (bf.header.isWB) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      cout << "garbage collector, yeah" << endl;
   } else {
      Partition& partition = getPartition(bf.header.pid);
      bf.reset();
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      partition.dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// returns a non-latched BufferFrame
BufferFrame& BufferManager::resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
{
   if (swip_value.isHOT()) {
      BufferFrame& bf = swip_value.bfRef();
      swip_guard.recheck();
      return bf;
   } else if (swip_value.isCOOL()) {
      BufferFrame* bf = swip_value.bfPtrAsHot();
      swip_guard.recheck();
      OptimisticGuard bf_guard(bf->header.latch, true);
      ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);  // parent
      ExclusiveGuard bf_x_guard(bf_guard);                // child
      // MyNote:: this swizzles the swip as it is reference
      bf->header.state = BufferFrame::STATE::HOT;
      swip_value.warm();
      return swip_value.bfRef();
   }
   // -------------------------------------------------------------------------------------
   swip_guard.unlock();  // otherwise we would get a deadlock, P->G, G->P
   const PID pid = swip_value.asPageID();
   // -------------------------------------------------------------------------------------
   if (isPageInBufferPool(pid)) {
      auto info = pageInBufferFrame(pid);
      auto bf = info.bf;
      // this should be used only with the parent swip
      // ensure(bf.header.state == BufferFrame::STATE::LOADED || bf.header.state == BufferFrame::STATE::UNLINKED_HOT ||
      //  bf.header.state == BufferFrame::STATE::UNLINKED_COOL);
      swip_guard.recheck();
      if (bf->header.state == BufferFrame::STATE::UNLINKED_COOL || bf->header.state == BufferFrame::STATE::UNLINKED_HOT) {
         OptimisticGuard bf_guard(bf->header.latch, true);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);  // parent
         ExclusiveGuard bf_x_guard(bf_guard);                // child
         // MyNote:: this swizzles the swip as it is reference
         bf->header.state = BufferFrame::STATE::HOT;
         // swip_value.evict(pid);
         swip_value.warm(bf);
         ensure(swip_value.bfPtr() == bf);
         return *bf;
      }
   }
   Partition& partition = getPartition(pid);
   JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
   swip_guard.recheck();
   assert(!swip_value.isHOT());
   // -------------------------------------------------------------------------------------
   // MyNote:: checking the page id is already loaded
   auto frame_handler = partition.io_ht.lookup(pid);
   if (!frame_handler) {
      // MyNote:: needs to write some page back before loading new page
      // std::cout << "Did not find the page id in coolling state pid: " << pid << std::endl;
      BufferFrame* bfptr = nullptr;
      u64 failcount = 0;
      volatile u32 mask = 1;
      while (bfptr == nullptr) {
         jumpmuTry()
         {
            bfptr = &(randomPartition().dram_free_list.tryPop(g_guard));  // EXP
            jumpmu_break;
         }
         jumpmuCatch()
         {
            DEBUG_BLOCK()
            {
               auto free_dram = freeDRAM();
               if (free_dram == 0) {
                  failcount++;
                  std::cout << "Free BufferFrame 0 encountered time: " << failcount << " pid: " << pid << std::endl;
               }
            }
            BACKOFF_STRATEGIES();
            swip_guard.recheck();
            g_guard->lock();
         }
      }
      BufferFrame& bf = *bfptr;
      // MyNote:: io queue
      IOFrame& io_frame = partition.io_ht.insert(pid);
      assert(bf.header.state == BufferFrame::STATE::FREE);
      bf.header.latch.assertNotExclusivelyLatched();
      // -------------------------------------------------------------------------------------
      io_frame.state = IOFrame::STATE::READING;
      io_frame.readers_counter = 1;
      io_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      // MyNote:: loads the page
      readPageSync(pid, bf.page);
      // std::cout << "Loaded pid: " << pid <<  std::endl;

      COUNTERS_BLOCK()
      {
         // WorkerCounters::myCounters().page_read[bf.page.dt_id]++;
         // WorkerCounters::myCounters().dt_misses_counter[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      assert(bf.page.magic_debugging_number == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      assert(!bf.header.isWB);
      bf.header.lastWrittenGSN = bf.page.GSN;
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      // TODO::disable attached segments in BufferFrame
      // auto itr = BMC::attached_segments.find(pid);
      // if (itr != BMC::attached_segments.end()) {
      //    bf.header.attached_segments = &(itr->second);
      // }
      // TODO::disable attached segments in BufferFrame
      auto mitr = BMC::leaf_node_models.find(pid);
      if (mitr != BMC::leaf_node_models.end()) {
         bf.header.model = mitr->second;
      }

      trackPID(pid, &bf);  // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         io_frame.mutex.unlock();
         swip_value.warm(&bf);
         bf.header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                     // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            // MyNote:: removed from io queue should be added to bf_ht
            partition.io_ht.remove(pid);
         }
         jumpmu_return bf;
      }
      jumpmuCatch()
      {
         // Change state to ready
         g_guard->lock();
         io_frame.bf = &bf;
         io_frame.state = IOFrame::STATE::READY;
         // -------------------------------------------------------------------------------------
         g_guard->unlock();
         io_frame.mutex.unlock();
         // -------------------------------------------------------------------------------------
         // MyNote:: Rethrowing
         DEBUG_JUMP()
         {
            std::cout << "Jump from resolveSwip::after reading from bufferframe" << LINE_NO() << std::endl;
         }
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   IOFrame& io_frame = frame_handler.frame();
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READING) {
      io_frame.readers_counter++;  // incremented while holding partition lock
      g_guard->unlock();
      io_frame.mutex.lock();
      io_frame.mutex.unlock();
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         g_guard->lock();
         if (io_frame.readers_counter == 0) {
            partition.io_ht.remove(pid);
         }
         g_guard->unlock();
      }
      // -------------------------------------------------------------------------------------
      DEBUG_JUMP()
      {
         std::cout << "Jump from resolveSwip:: as the ioframe is in reading state" << LINE_NO() << std::endl;
      }
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READY) {
      // -------------------------------------------------------------------------------------
      BufferFrame* bf = io_frame.bf;
      {
         // We have to exclusively lock the bf because the page provider thread will
         // try to evict them when its IO is done
         bf->header.latch.assertNotExclusivelyLatched();
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         OptimisticGuard bf_guard(bf->header.latch);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         ExclusiveGuard bf_x_guard(bf_guard);
         // -------------------------------------------------------------------------------------
         io_frame.bf = nullptr;
         assert(bf->header.pid == pid);
         swip_value.warm(bf);
         assert(swip_value.isHOT());
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         bf->header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                      // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         } else {
            io_frame.state = IOFrame::STATE::TO_DELETE;
         }
         g_guard->unlock();
         // -------------------------------------------------------------------------------------
         return *bf;
      }
   }
   if (io_frame.state == IOFrame::STATE::TO_DELETE) {
      if (io_frame.readers_counter == 0) {
         partition.io_ht.remove(pid);
      }
      g_guard->unlock();
      DEBUG_JUMP()
      {
         std::cout << "jump from resolveSwip:: because ioFrame is in to delete state" << LINE_NO() << std::endl;
      }
      jumpmu::jump();
   }
   ensure(false);
}

bool BufferManager::isPageInBufferPool(PID pid)
{
#ifdef TRACK_WITH_HT
   auto bf_itr = bf_ht.find(pid);
   if (bf_itr != bf_ht.end()) {
      return true;
   }
   return false;
#else
   if (bf_vt.size() > pid) {
      return bf_vt[pid].bf != nullptr;
   } else {
      return false;
   }
#endif
}
bool BufferManager::trackPID(PID pid, BufferFrame* bf, KEY lower_fence, KEY upper_fence)
{
#ifdef TRACK_WITH_HT
   {
      // JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      // std::unique_lock<std::mutex> bf_guard(bf_mutex);
      bf_ht.insert(std::make_pair(pid, {bf, lower_fence, upper_fence}));
   }
#else

   auto size = bf_vt.size();
   // we need to comment this out temporarily for static array to be used
   while (size <= pid) {
      // std::cout << "bf_vt_size: " << size << " pid: " << pid << " bf_vt needs doubling" << std::endl;
      size = size * 2;
   }
   {
      // JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      if (size > bf_vt.size()) {
         bf_vt.resize(size);
      }
      ensure(pid < bf_vt.size());
      ensure(bf_vt.size() >= size);
      bf_vt[pid] = {bf, lower_fence, upper_fence};
   }
   return false;
#endif
}
bool BufferManager::trackPID(PID pid, BufferFrame* bf)
{
   auto info = pageInBufferFrame(pid);
#ifdef TRACK_WITH_HT
   {
      // JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      // std::unique_lock<std::mutex> bf_guard(bf_mutex);
      bf_ht.insert(std::make_pair(pid, {bf, lower_fence, upper_fence}));
   }
#else

   auto size = bf_vt.size();
   // we need to comment this out temporarily for static array to be used
   while (size <= pid) {
      // std::cout << "bf_vt_size: " << size << " pid: " << pid << " bf_vt needs doubling" << std::endl;
      size = size * 2;
   }
   {
      // JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      if (size > bf_vt.size()) {
         bf_vt.resize(size);
      }
      ensure(pid < bf_vt.size());
      ensure(bf_vt.size() >= size);
      bf_vt[pid] = {bf, info.lower_fence, info.upper_fence};
   }
   return false;
#endif
}

bool BufferManager::untrackPID(PID pid)
{
#ifdef TRACK_WITH_HT
   auto bf_ptr = bf_ht.find(pid);
   if (bf_ptr != bf_ht.end()) {
      JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      // std::unique_lock<std::mutex> bf_guard(bf_mutex);
      bf_ht.erase(bf_ptr);
      return true;
   } else {
      return false;
   }
#else
   if (bf_vt.size() > pid) {
      // JMUW<std::unique_lock<std::mutex>> g_guard(bf_mutex);
      // std::unique_lock<std::mutex> bf_guard(bf_mutex);
      auto info = bf_vt[pid];
      HybridPageGuard<leanstore::storage::btree::BTreeNode> guard(info.bf);
      auto lower_fence = utils::u8_to<KEY>(guard->getLowerFenceKey(), sizeof(KEY));
      auto upper_fence = utils::u8_to<KEY>(guard->getUpperFenceKey(), sizeof(KEY));
      bf_vt[pid] = {nullptr, lower_fence, upper_fence};
      ensure(bf_vt[pid].bf == nullptr);
      return true;
   } else {
      return false;
   }
#endif
}

bool BufferManager::getPage(PID pid, BufferFrame* bf)
{
   /*
   auto bf_itr = bf_ht.find(pid);
   if (bf_itr != bf_ht.end()) {
      auto& bf = *(bf_itr->second);
      if (bf.header.state == bufferframe::state::cool) {
         return bf;
      }
      else if(bf.header.state == bufferframe::state::hot) {
         return bf;
      }
   }
   else {
      */
   readPageSync(pid, bf->page);
   {
      OptimisticGuard bf_guard(bf->header.latch, true);
      ExclusiveGuard bf_x_guard(bf_guard);
      bf->header.state = BufferFrame::STATE::UNLINKED_HOT;
      DEBUG_BLOCK()
      {
         // std::cout << "Reading page in BufferPool in UNLINKED_HOT state: " << bf << std::endl;
      }
      ensure(bf->header.state == BufferFrame::STATE::UNLINKED_HOT);
   }
   return true;
   // }
}

BufferInfo BufferManager::getPageinBufferPool(PID pid)
{
#ifdef INSTRUMENT_CODE
   static AvgFunctionTimer avgtimer("getPageInBufferPool");
   FunctionTimer timer("getPageInBufferPool", avgtimer);
#endif
   if (auto info = pageInBufferFrame(pid); info.bf != nullptr) {
      auto bf = info.bf;
      // ensure(bf->header.pid == pid);
      if (bf->header.state == BufferFrame::STATE::HOT || bf->header.state == BufferFrame::STATE::UNLINKED_HOT ||
          bf->header.state == BufferFrame::STATE::LOADED) {
         // if ((static_cast<T>(bf->header.state) & static_cast<T>(BufferFrame::STATE::HOTMASK)) != 0) {
         return info;
      } else if (bf->header.state == BufferFrame::STATE::UNLINKED_COOL) {
         OptimisticGuard bf_guard(bf->header.latch, true);
         ExclusiveGuard bf_x_guard(bf_guard);
         bf->header.state = BufferFrame::STATE::UNLINKED_HOT;
         return info;
      } else if (bf->header.state == BufferFrame::STATE::COOL) {
         // ignore for now
         return info;
      } else {
         return {nullptr, info.lower_fence, info.upper_fence};
      };
   }
   // if (auto bf = pageInBufferFrame(pid); bf != nullptr) {
   //    switch (bf->header.state) {
   //       case BufferFrame::STATE::HOT:
   //       case BufferFrame::STATE::UNLINKED_HOT:
   //       case BufferFrame::STATE::LOADED:
   //          return bf;

   //       case BufferFrame::STATE::UNLINKED_COOL: {
   //          OptimisticGuard bf_guard(bf->header.latch, true);
   //          ExclusiveGuard bf_x_guard(bf_guard);
   //          bf->header.state = BufferFrame::STATE::UNLINKED_HOT;
   //          return bf;
   //       }
   //       case BufferFrame::STATE::COOL:
   //          // Ignore for now
   //          return bf;
   //       default:
   //          return nullptr;
   //    }
   // }

   Partition& partition = getPartition(pid);
   JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
   BufferFrame* bfptr = nullptr;
   u64 failcount = 0;
   volatile u32 mask = 1;
   while (bfptr == nullptr) {
      jumpmuTry()
      {
         bfptr = &(randomPartition().dram_free_list.tryPop(g_guard));  // EXP
         jumpmu_break;
      }
      jumpmuCatch()
      {
         DEBUG_BLOCK()
         {
            auto free_dram = freeDRAM();
            if (free_dram == 0) {
               failcount++;
               // if (failcount == 0)
               // std::cout << "Free BufferFrame 0 encountered time: " << failcount << " pid: " << pid << LINE_NO() << std::endl;
            }
         }
         BACKOFF_STRATEGIES();
         g_guard->lock();
      }
   }
   // MyNote:: io queueS
   auto bf = bfptr;
   // assert(bf->header.state == BufferFrame::STATE::FREE);
   IOFrame& io_frame = partition.io_ht.insert(pid);
   bf->header.latch.assertNotExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   io_frame.state = IOFrame::STATE::READING;
   io_frame.readers_counter = 1;
   io_frame.mutex.lock();
   // -------------------------------------------------------------------------------------
   g_guard->unlock();
   // -------------------------------------------------------------------------------------
   // MyNote:: loads the page
   getPage(pid, bf);
   // std::cout << "Loaded pid: " << pid <<  std::endl;

   COUNTERS_BLOCK()
   {
      // WorkerCounters::myCounters().page_read[bf->page.dt_id]++;
      // WorkerCounters::myCounters().dt_misses_counter[bf->page.dt_id]++;
      if (FLAGS_trace_dt_id >= 0 && bf->page.dt_id == FLAGS_trace_dt_id &&
          utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
         utils::printBackTrace();
      }
   }
   // assert(bf->page.magic_debugging_number == pid);
   // -------------------------------------------------------------------------------------
   // ATTENTION: Fill the BF
   // assert(!bf->header.isWB);
   bf->header.lastWrittenGSN = bf->page.GSN;
   bf->header.pid = pid;
   // MyNote:: add the entry to bf_ht
   trackPID(pid, bf);
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
      io_frame.mutex.unlock();
      // -------------------------------------------------------------------------------------
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         // MyNote:: removed from io queue should be added to bf_ht
         partition.io_ht.remove(pid);
      }
      HybridPageGuard<leanstore::storage::btree::BTreeNode> node(bf);
      KEY lower_fence = 0;
      KEY upper_fence = 0;
      jumpmu_return{bf, lower_fence, upper_fence};
   }
   jumpmuCatch()
   {
      // Change state to ready
      g_guard->lock();
      io_frame.bf = bf;
      io_frame.state = IOFrame::STATE::READY;
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      io_frame.mutex.unlock();
      // -------------------------------------------------------------------------------------
      // MyNote:: Rethrowing
      DEBUG_JUMP()
      {
         std::cout << "Jump from resolveSwip::after reading from bufferframe" << LINE_NO() << std::endl;
      }
      jumpmu::jump();
   }
}

// namespace storage
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   // MyNote:: Read detected
   // std::cout << "Reading page sync" << std::endl;
   assert(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_left > 0);
      bytes_left -= bytes_read;
   } while (bytes_left > 0);
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().read_operations_counter++;
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getPartitionID(PID pid)
{
   return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
Partition& BufferManager::getPartition(PID pid)
{
   const u64 partition_i = getPartitionID(pid);
   assert(partition_i < partitions_count);
   return partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
      MYPAUSE();
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   free(partitions);
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   munmap(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
// State
std::unordered_map<std::string, std::string> BufferManager::serialize()
{
   // TODO: correctly serialize ranges of used pages
   std::unordered_map<std::string, std::string> map;
   PID max_pid = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      max_pid = std::max<PID>(getPartition(p_i).next_pid, max_pid);
   }
   map["max_pid"] = std::to_string(max_pid);
   return map;
}
// -------------------------------------------------------------------------------------
void BufferManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   PID max_pid = std::stod(map["max_pid"]);
   max_pid = (max_pid + (partitions_count - 1)) & ~(partitions_count - 1);
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      getPartition(p_i).next_pid = max_pid + p_i;
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::BufferPoolUseInf()
{
   int cool = 0;
   int hot = 0;
   int free = 0;
   int loaded = 0;
   int unlinked_cool = 0;
   int unlinked_hot = 0;
   int leaf = 0;
   int inner = 0;
   int filled = 0;
   for (int i = 0; i < dram_pool_size; i++) {
      if (bfs[i].header.state != BufferFrame::STATE::FREE) {
         HybridPageGuard<leanstore::storage::btree::BTreeNode> node(bfs + i);
         if (node->is_leaf) {
            leaf++;
         } else {
            inner++;
         }
         filled++;
      }

      if (bfs[i].header.state == BufferFrame::STATE::COOL) {
         cool++;
      } else if (bfs[i].header.state == BufferFrame::STATE::FREE) {
         free++;
      } else if (bfs[i].header.state == BufferFrame::STATE::HOT) {
         hot++;
      } else if (bfs[i].header.state == BufferFrame::STATE::LOADED) {
         loaded++;
      } else if (bfs[i].header.state == BufferFrame::STATE::UNLINKED_COOL) {
         unlinked_cool++;
      } else if (bfs[i].header.state == BufferFrame::STATE::UNLINKED_HOT) {
         unlinked_hot++;
      }
   }
   std::cout << "BufferFrame usage: Total: " << dram_pool_size << " cool: " << cool << " free: " << free << " hot: " << hot << " loaded: " << loaded
             << " unliked_cool: " << unlinked_cool << " unlinked_hot: " << unlinked_hot << std::endl;
   std::cout << "BufferFrame leaf: " << leaf << " inner: " << inner << " filled: " << filled << std::endl;
}
//-------------------------------------------------------------------------------------
void BufferManager::clearAttachedSegments()
{
   for (auto i = 0; i < dram_pool_size; i++) {
      bfs[i].header.attached_segments = nullptr;
   }
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
ska::flat_hash_map<PID, std::vector<size_t>> BMC::attached_segments;
ska::flat_hash_map<PID, learnedindex<KEY>> BMC::leaf_node_models;