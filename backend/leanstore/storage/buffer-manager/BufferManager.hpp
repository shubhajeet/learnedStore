#pragma once
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <sys/mman.h>

#include <cstring>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include "flat_hash_map.hpp"
#include "leanstore/compileConst.hpp"

#ifdef TRACK_WITH_HT
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_map.h>
#include <bytell_hash_map.hpp>
#include <flat_hash_map.hpp>
#include <sparsehash/sparse_hash_map>
#include <unordered_map>

struct MurmurHash64A {
   size_t operator()(PID k) const
   {
      // MurmurHash64A
      const uint64_t m = 0xc6a4a7935bd1e995ull;
      const int r = 47;
      uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
   }
};
struct UIntHashCompare {
   size_t operator()(const uint32_t& key) const { return key; }

   bool operator()(const uint32_t& key1, const uint32_t& key2) const { return key1 == key2; }
};
#else
#include <tbb/concurrent_vector.h>
#include <vector>
#include "leanstore/utils/array.hpp"
#include "leanstore/utils/staticarray.hpp"
#endif

struct BufferInfo {
   leanstore::storage::BufferFrame* bf;
   KEY lower_fence, upper_fence;
};
// -------------------------------------------------------------------------------------
namespace leanstore
{
class LeanStore;
namespace profiling
{
class BMTable;
}
namespace storage
{
// -------------------------------------------------------------------------------------
/*
 * Swizzle a page:
 * 1- bf_s_lock global bf_s_lock
 * 2- if it is in cooling stage:
 *    a- yes: bf_s_lock write (spin till you can), remove from stage, swizzle in
 *    b- no: set the state to IO, increment the counter, hold the mutex, p_read
 * 3- if it is in IOFlight:
 *    a- increment counter,
 */
class BufferManager
{
  private:
   friend class leanstore::LeanStore;
   friend class leanstore::profiling::BMTable;
   // -------------------------------------------------------------------------------------
  public:
   BufferFrame* bfs;
   u64 dram_pool_size;  // total number of dram buffer frames
  private:
   // -------------------------------------------------------------------------------------
   const int ssd_fd;
   // -------------------------------------------------------------------------------------
   // Free  Pages
   const u8 safety_pages = 10;               // we reserve these extra pages to prevent segfaults
   atomic<u64> ssd_freed_pages_counter = 0;  // used to track how many pages did we really allocate
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   u64 partitions_count;
   u64 partitions_mask;
   Partition* partitions;
   // MyNote -------------------------------------------------------------------------------
   std::mutex bf_mutex;
#ifdef TRACK_WITH_HT
   // ska::bytell_hash_map<PID, BufferFrame*> bf_ht;
   ska::flat_hash_map<PID, BufferInfo, MurmurHash64A> bf_ht;
   // google::sparse_hash_map<PID, BufferFrame*, MurmurHash64A> bf_ht;
   // std::unordered_map<PID, BufferFrame*, MurmurHash64A> bf_ht;
   // tbb::concurrent_unordered_map<PID, BufferFrame*, MurmurHash64A> bf_ht;
   // tbb::concurrent_unordered_map<PID, BufferFrame*> bf_ht;
   // tbb::concurrent_hash_map<PID, BufferFrame*> bf_ht;
#else
   // std::vector<BufferInfo> bf_vt;
   // DynamicArray<BufferInfo> bf_vt;
   // StaticArray<BufferInfo, 50000000> bf_vt;
   tbb::concurrent_vector<BufferInfo> bf_vt;
#endif
  private:
   // -------------------------------------------------------------------------------------
   // Threads managements
   void pageProviderThread(u64 p_begin, u64 p_end);  // [p_begin, p_end)
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   BufferFrame& randomBufferFrame();
   struct BufferInfo getBufferFrame(PID pid);
   Partition& getPartition(PID);
   u64 getPartitionID(PID);

  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame& allocatePage();
   inline BufferFrame& tryFastResolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
   {
      // MyNote:: hot
      if (swip_value.isHOT()) {
         BufferFrame& bf = swip_value.bfRef();
         swip_guard.recheck();
         return bf;
      } else {
         // MyNote:: resolve swip
         return resolveSwip(swip_guard, swip_value);
      }
   }
   BufferFrame& resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value);
   // -------------------------------------------------------------------------------------
   /*
    * Tracks page id is present in bufferfram
    */
   bool isPageInBufferPool(PID pid);
   inline BufferInfo pageInBufferFrame(PID pid)
   {
      // -------------------------------------------------------------------------------------
      // MyNote:: find the page in bf_ht
      // BufferFrame* bf = nullptr;
#ifdef TRACK_WITH_HT
      auto bf_itr = bf_ht.find(pid);
      return (bf_itr != bf_ht.end()) ? bf_itr->second : nullptr;
      // if (bf_itr != bf_ht.end()) {
      //    bf = bf_itr->second;
      // } else {
      //    return nullptr;
      // }
#else
      BufferInfo nullinfo = {nullptr, 0, 0};
      return (bf_vt.size() > pid) ? bf_vt[pid] : nullinfo;
      // bf = bf_vt[pid];
      // if (bf == nullptr) {
      //    return nullptr;
      // }
#endif
   };
   bool trackPID(PID pid, BufferFrame* bf, KEY lower_fence, KEY upper_fence);
   bool trackPID(PID pid, BufferFrame* bf);
   bool untrackPID(PID pid);
   // -------------------------------------------------------------------------------------
   BufferInfo getPageinBufferPool(PID pid);

   bool getPage(PID pid, BufferFrame* bf);
   void reclaimPage(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   void stopBackgroundThreads();
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address
    * temporarily 2- if not, then posix_check if it exists in cooling stage
    * queue, yes? remove it from the queue and return the buffer frame 3- in
    * anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random
    * BufferFrame (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8* destination);
   void readPageAsync(PID pid, u8* destination, std::function<void()> callback);
   void fDataSync();
   // -------------------------------------------------------------------------------------
   void clearSSD();
   void restore();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
   // -------------------------------------------------------------------------------------
   u64 getPoolSize() { return dram_pool_size; }
   DTRegistry& getDTRegistry() { return DTRegistry::global_dt_registry; }
   u64 consumedPages();
   u64 freeDRAM();
   BufferFrame& getContainingBufferFrame(const u8*);  // get the buffer frame containing the given ptr address
   void clearAttachedSegments();
   void BufferPoolUseInf();
};  // namespace storage
// -------------------------------------------------------------------------------------
class BMC
{
  public:
   static BufferManager* global_bf;
   static ska::flat_hash_map<PID, std::vector<size_t>> attached_segments;
   static ska::flat_hash_map<PID, learnedindex<KEY>> leaf_node_models;
};
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
