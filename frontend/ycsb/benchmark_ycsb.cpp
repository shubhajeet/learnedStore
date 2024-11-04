#include <gflags/gflags.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <bitset>
#include <cassert>
#include <condition_variable>  // std::condition_variable
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>  // std::mutex
#include <sstream>
#include <thread>  // std::thread
#include <vector>

#include <tbb/tbb.h>

#include "histogram.h"
#include "logger.h"
#include "test_util.h"

#define TRACE_DUMP false
#define YCSB_USE_READ_TRACE
#define SEG_IN_INSERT
// #define TRACE_SOSD
#define DUMP_EACH_LATENCY
#define USE_SLOT_KEYS
// ========== LEANSTORE HEADER ==========
#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/compileConst.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// === RadixSpline ===
// #include "rs/builder.h"
// #include "rs/radix_spline.h"

using namespace leanstore;
using namespace util;
using YCSBKey = KEY;
// This is to be set for large dataset experiment 2O0M for 200GB dataset
// using YCSBPayload = BytesPayload<512>;
// This is normal wokrload experiment
using YCSBPayload = BytesPayload<128>;
// using YCSBPayload = BytesPayload<256>;
// This is for large dataset when we want it to still be in memory dataset
// using YCSBPayload = BytesPayload<8>;
// using YCSBPayload = BytesPayload<64>;
// using YCSBPayload = BytesPayload<512>;
// using YCSBPayload = BytesPayload<1024>;
// using YCSBPayload = BytesPayload<1280>;
// using YCSBPayload = BytesPayload<2048>;

#define likely(x) (__builtin_expect(false || (x), true))
#define unlikely(x) (__builtin_expect(x, 0))

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_uint32(batch, 1000, "report batch");
DEFINE_uint32(readtime, 0, "if 0, then we read all keys");
DEFINE_uint64(report_interval, 0, "Report interval in seconds");
DEFINE_uint64(stats_interval, 200000000, "Report interval in ops");
DEFINE_uint64(value_size, 8, "The value size");
DEFINE_uint64(num, 10 * 1000000LU, "Number of total record");
DEFINE_uint64(read, 0, "Number of read operations");
DEFINE_uint64(write, 1 * 1000000, "Number of read operations");
DEFINE_uint64(range_length, 400, "Number of read operations");
DEFINE_bool(hist, false, "");
DEFINE_string(benchmarks, "load,readall", "");
DEFINE_string(tracefile, "randomtrace.data", "");
DEFINE_uint32(step, 0, "0 for random keys while larger than 0 means sequential keys with given step");
DEFINE_bool(seq_operation, false, "benchmark should be sequential");
DEFINE_bool(seq_write_operation, false, "benchmark write should be sequential");
DEFINE_double(zipfian_constant, 0.99, "Zipfian constant");

namespace
{

class Stats
{
  public:
   int tid_;
   double start_;
   double finish_;
   double seconds_;
   double next_report_time_;
   double last_op_finish_;
   unsigned last_level_compaction_num_;
   util::HistogramImpl hist_;

   uint64_t done_;
   uint64_t last_report_done_;
   uint64_t last_report_finish_;
   uint64_t next_report_;
   std::string message_;

   Stats() { Start(); }
   explicit Stats(int id) : tid_(id) { Start(); }

   void Start()
   {
      start_ = NowMicros();
      next_report_time_ = start_ + FLAGS_report_interval * 1000000;
      next_report_ = 100;
      last_op_finish_ = start_;
      last_report_done_ = 0;
      last_report_finish_ = start_;
      last_level_compaction_num_ = 0;
      done_ = 0;
      seconds_ = 0;
      finish_ = start_;
      message_.clear();
      hist_.Clear();
   }

   void Merge(const Stats& other)
   {
      hist_.Merge(other.hist_);
      done_ += other.done_;
      seconds_ += other.seconds_;
      if (other.start_ < start_)
         start_ = other.start_;
      if (other.finish_ > finish_)
         finish_ = other.finish_;

      // Just keep the messages from one thread
      if (message_.empty())
         message_ = other.message_;
   }

   void Stop()
   {
      finish_ = NowMicros();
      seconds_ = (finish_ - start_) * 1e-6;
      ;
   }

   void StartSingleOp() { last_op_finish_ = NowMicros(); }

   void PrintSpeed()
   {
      uint64_t now = NowMicros();
      int64_t usecs_since_last = now - last_report_finish_;

      std::string cur_time = TimeToString(now / 1000000);
      /*
      printf (
          "%s ... thread %d: (%lu,%lu) ops and "
          "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
          cur_time.c_str (), tid_, done_ - last_report_done_, done_,
          (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
          done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
          (now - start_) / 1000000.0);
          */
      printf("[Epoch] %d,%lu,%lu,%.4f,%.4f\n", tid_, done_ - last_report_done_, done_, (now - last_report_finish_) / 1000000.0,
             (now - start_) / 1000000.0);

      last_report_finish_ = now;
      last_report_done_ = done_;
      fflush(stdout);
   }

   static void AppendWithSpace(std::string* str, const std::string& msg)
   {
      if (msg.empty())
         return;
      if (!str->empty()) {
         str->push_back(' ');
      }
      str->append(msg.data(), msg.size());
   }

   void AddMessage(const std::string& msg) { AppendWithSpace(&message_, msg); }

   inline void FinishedBatchOp(size_t batch)
   {
      double now = NowNanos();
      last_op_finish_ = now;
      done_ += batch;
      if (unlikely(done_ >= next_report_)) {
         if (next_report_ < 1000)
            next_report_ += 100;
         else if (next_report_ < 5000)
            next_report_ += 500;
         else if (next_report_ < 10000)
            next_report_ += 1000;
         else if (next_report_ < 50000)
            next_report_ += 5000;
         else if (next_report_ < 100000)
            next_report_ += 10000;
         else if (next_report_ < 500000)
            next_report_ += 50000;
         else
            next_report_ += 100000;
         fprintf(stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

         if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
            PrintSpeed();
            return;
         }
         fflush(stderr);
         fflush(stdout);
      }

      if (FLAGS_report_interval != 0 && NowMicros() > next_report_time_) {
         next_report_time_ += FLAGS_report_interval * 1000000;
         PrintSpeed();
      }
   }

   inline void FinishedSingleOp()
   {
      double now = NowNanos();
      last_op_finish_ = now;

      done_++;
      if (done_ >= next_report_) {
         if (next_report_ < 1000)
            next_report_ += 100;
         else if (next_report_ < 5000)
            next_report_ += 500;
         else if (next_report_ < 10000)
            next_report_ += 1000;
         else if (next_report_ < 50000)
            next_report_ += 5000;
         else if (next_report_ < 100000)
            next_report_ += 10000;
         else if (next_report_ < 500000)
            next_report_ += 50000;
         else
            next_report_ += 100000;
         fprintf(stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

         if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
            PrintSpeed();
            return;
         }
         fflush(stderr);
         fflush(stdout);
      }

      if (FLAGS_report_interval != 0 && NowMicros() > next_report_time_) {
         next_report_time_ += FLAGS_report_interval * 1000000;
         PrintSpeed();
      }
   }

   std::string TimeToString(uint64_t secondsSince1970)
   {
      const time_t seconds = (time_t)secondsSince1970;
      struct tm t;
      int maxsize = 64;
      std::string dummy;
      dummy.reserve(maxsize);
      dummy.resize(maxsize);
      char* p = &dummy[0];
      localtime_r(&seconds, &t);
      snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
      return dummy;
   }

   void Report(const util::Slice& name, bool print_hist = false)
   {
      // Pretend at least one op was done in case we are running a benchmark
      // that does not call FinishedSingleOp().
      if (done_ < 1)
         done_ = 1;

      std::string extra;

      AppendWithSpace(&extra, message_);

      double elapsed = (finish_ - start_) * 1e-6;

      double throughput = (double)done_ / elapsed;

      printf("%-12s : %11.3f micros/op %lf Mops/s total_time: %lf sec ;%s%s\n", name.ToString().c_str(), elapsed * 1e6 / done_,
             throughput / 1000 / 1000, elapsed, (extra.empty() ? "" : " "), extra.c_str());
      if (print_hist) {
         fprintf(stdout, "Nanoseconds per op:\n%s\n", hist_.ToString().c_str());
      }

      fflush(stdout);
      fflush(stderr);
   }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
   std::mutex mu;
   std::condition_variable cv;
   int total;

   // Each thread goes through the following states:
   //    (1) initializing
   //    (2) waiting for others to be initialized
   //    (3) running
   //    (4) done

   int num_initialized;
   int num_done;
   bool start;

   SharedState(int total) : total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
   int tid;  // 0..n-1 when running in n threads
   // Random rand;         // Has different seeds for different threads
   Stats stats;
   SharedState* shared;
   YCSBGenerator ycsb_gen;
   ThreadState(int index) : tid(index), stats(index) {}
};

class Duration
{
  public:
   Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0)
   {
      max_seconds_ = max_seconds;
      max_ops_ = max_ops;
      ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
      ops_ = 0;
      start_at_ = NowMicros();
   }

   inline int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

   inline bool Done(int64_t increment)
   {
      if (increment <= 0)
         increment = 1;  // avoid Done(0) and infinite loops
      ops_ += increment;

      if (max_seconds_) {
         // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
         auto granularity = 1000;
         if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
            uint64_t now = NowMicros();
            return ((now - start_at_) / 1000000) >= max_seconds_;
         } else {
            return false;
         }
      } else {
         return (max_ops_ != 0) ? ops_ > max_ops_ : false;
      }
   }

   inline int64_t Ops() { return ops_; }

  private:
   uint64_t max_seconds_;
   int64_t max_ops_;
   int64_t ops_per_stage_;
   int64_t ops_;
   uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace(std::string s)
{
   size_t start = 0;
   while (start < s.size() && isspace(s[start])) {
      start++;
   }
   size_t limit = s.size();
   while (limit > start && isspace(s[limit - 1])) {
      limit--;
   }
   return std::string(s.data() + start, limit - start);
}
#endif

}  // namespace

#define POOL_SIZE (1073741824L * 100L)  // 100GB

class Benchmark
{
  public:
   // uint64_t num_;
   int value_size_;  // size of value
   size_t reads_;    // number of read operations
   size_t writes_;   // number of write operations
   KeyTrace<YCSBKey>* read_key_trace_;
   KeyTrace<YCSBKey>* write_key_trace_;
   size_t read_trace_size_;
   size_t write_trace_size_;
   LeanStore db;
   unique_ptr<BTreeInterface<YCSBKey, YCSBPayload>> adapter;
   leanstore::storage::btree::BTreeLL* btree_ptr = nullptr;
   // rsindex::RadixSpline<YCSBKey> rsindex;
   std::vector<YCSBKey> mappingkeys;

   Benchmark()
       : value_size_(FLAGS_value_size),
         reads_(FLAGS_read),
         writes_(FLAGS_write),
         read_key_trace_(nullptr),
         write_key_trace_(nullptr),
         read_trace_size_(0),
         write_trace_size_(0)
   {
      if (FLAGS_recover) {
         btree_ptr = &db.retrieveBTreeLL("ycsb");
      } else {
         btree_ptr = &db.registerBTreeLL("ycsb");
      }
      adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr));
      db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
      db.startProfilingThread();
   }

   ~Benchmark() {}

   void Run()
   {
      read_trace_size_ = FLAGS_num;
      printf("key trace size: %lu\n", read_trace_size_);
      read_key_trace_ = new KeyTrace<YCSBKey>();
      write_key_trace_ = new KeyTrace<YCSBKey>();
      if (reads_ == 0) {
         reads_ = read_key_trace_->keys_.size();
      }
      PrintHeader();
      // run benchmark
      bool print_hist = false;
      const char* benchmarks = FLAGS_benchmarks.c_str();
      while (benchmarks != nullptr) {
         int thread = FLAGS_worker_threads;
         void (Benchmark::*method)(ThreadState*) = nullptr;
         const char* sep = strchr(benchmarks, ',');
         std::string name;
         if (sep == nullptr) {
            name = benchmarks;
            benchmarks = nullptr;
         } else {
            name = std::string(benchmarks, sep - benchmarks);
            benchmarks = sep + 1;
         }
         if (name == "load") {
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::DoWrite;  // write to the leanstore
         } else if (name == "loadsorted") {
            write_key_trace_->Sort();
            method = &Benchmark::DoWrite;
         } else if (name == "fastload") {
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::DoWriteFast;  // write to the leanstore
         } else if (name == "loadfastsorted") {
            write_key_trace_->Sort();
            method = &Benchmark::DoWriteFast;
         } else if (name == "genlinear") {
            thread = 1;
            method = &Benchmark::GenLinear;
         } else if (name == "genrandom") {
            thread = 1;
            method = &Benchmark::GenRandom;
         } else if (name == "writetracetoread") {
            thread = 1;
            method = &Benchmark::WriteToRead;
         } else if (name == "filterstepreadtrace") {
            thread = 1;
            method = &Benchmark::FilterStepReadTrace;
         } else if (name == "fasttrain") {
            thread = 1;
            method = &Benchmark::DoFastTrain;
         } else if (name == "train") {
            thread = 1;
            method = &Benchmark::DoTrain;
         } else if (name == "loadlat") {
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            print_hist = true;
            method = &Benchmark::DoWriteLat;
         } else if (name == "overwrite") {
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::DoOverWrite;
         } else if (name == "readrandom") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoRead;
         } else if (name == "simulatereadlat") {
            print_hist = true;
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoSimulateReadLat;
         } else if (name == "readall") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoReadAll;
         } else if (name == "readzip") {
            read_key_trace_->Randomize();
            // read_key_trace_->Sort();
            method = &Benchmark::DoReadZipf;
         } else if (name == "readzipwithrs") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoReadUseSegmentZipf;
         } else if (name == "readallwithseg") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadUseSegmentAll;
         } else if (name == "readzipwithseg") {
            std::cout << "Randomizing read key trace" << std::endl;
            read_key_trace_->Randomize();
            // read_key_trace_->Sort();
            method = &Benchmark::DoReadUseSegmentZipf;
         } else if (name == "readlatwithseg") {
            print_hist = true;
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadLatWithSeg;
         } else if (name == "readnon") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadNon;
         } else if (name == "readlat") {
            print_hist = true;
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadLat;
         } else if (name == "readzipflat") {
            print_hist = true;
            // read_key_trace_->Sort();
            std::cout << "Randomizing read key trace" << std::endl;
            read_key_trace_->Randomize();
            method = &Benchmark::DoReadZipfianLat;
         } else if (name == "readzipflatwithseg") {
            print_hist = true;
            // read_key_trace_->Sort();
            std::cout << "Randomizing read key trace" << std::endl;
            read_key_trace_->Randomize();
            method = &Benchmark::DoReadZipfianLatWithSeg;
         } else if (name == "readftaillat") {
            print_hist = true;
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadFtailLat;
         } else if (name == "readnonlat") {
            print_hist = true;
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            method = &Benchmark::DoReadNonLat;
         } else if (name == "ycsba") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBA;
         } else if (name == "ycsbaseg") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBA_Seg;
         } else if (name == "ycsbb") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBB;
         } else if (name == "ycsbbseg") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBB_Seg;
         } else if (name == "ycsbc") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBC;
         } else if (name == "ycsbd") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBD;
         } else if (name == "ycsbf") {
            if (!FLAGS_seq_operation) {
               std::cout << "Randomizing read key trace" << std::endl;
               read_key_trace_->Randomize();
            }
            if (!FLAGS_seq_write_operation) {
               std::cout << "Randomizing write key trace" << std::endl;
               write_key_trace_->Randomize();
            }
            method = &Benchmark::YCSBF;
         } else if (name == "statistics") {
            thread = 1;
            method = &Benchmark::Statistics;
         } else if (name == "readtraceload") {
            thread = 1;
            method = &Benchmark::DoReadTraceLoad;
         } else if (name == "writetraceload") {
            thread = 1;
            method = &Benchmark::DoWriteTraceLoad;
         } else if (name == "readtracesave") {
            thread = 1;
            method = &Benchmark::DoSaveTrace;
         } else if (name == "poolstats") {
            thread = 1;
            method = &Benchmark::DoPoolStats;
         } else if (name == "scanallasc") {
            thread = 1;
            method = &Benchmark::DoScanAllAsc;
         } else if (name == "scanallascseg") {
            thread = 1;
            method = &Benchmark::DoScanAllAscSeg;
         } else if (name == "scanasc") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoScanAsc;
         } else if (name == "scandesc") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoScanDesc;
         } else if (name == "scanascseg") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoScanAscSeg;
         } else if (name == "scandescseg") {
            if (!FLAGS_seq_operation)
               read_key_trace_->Randomize();
            method = &Benchmark::DoScanDescSeg;
         } else {
            std::cout << "unknown benchmark " << name << std::endl;
         }

         if (method != nullptr)
            RunBenchmark(thread, name, method, print_hist);
      }
   }

   void GenRandom(ThreadState* thread)
   {
      write_key_trace_->generate(FLAGS_num, 0, read_key_trace_);
      if constexpr (TRACE_DUMP) {
         for (auto i = 0; i < write_key_trace_->count_; i++) {
            std::cout << "key from key_trace: " << write_key_trace_->keys_[i] << std::endl;
         }
      }
      write_trace_size_ = write_key_trace_->count_;
   }

   void GenLinear(ThreadState* thread)
   {
      write_key_trace_->generate(FLAGS_num, FLAGS_step, read_key_trace_);
      if constexpr (TRACE_DUMP) {
         for (auto i = 0; i < write_key_trace_->count_; i++) {
            std::cout << "key from key_trace: " << write_key_trace_->keys_[i] << std::endl;
         }
      }
      write_trace_size_ = write_key_trace_->count_;
   }

   void WriteToRead(ThreadState* thread)
   {
      std::cout << "inital state read_trace_size_: " << read_trace_size_ << " write_trace_size_: " << write_trace_size_ << std::endl;
      read_key_trace_->keys_.insert(read_key_trace_->keys_.end(), write_key_trace_->keys_.begin(), write_key_trace_->keys_.end());
      read_key_trace_->count_ = read_key_trace_->keys_.size();
      write_key_trace_->keys_.clear();
      write_key_trace_->count_ = write_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->count_;
      read_trace_size_ = read_key_trace_->count_;
      std::cout << "final state read_trace_size_: " << read_trace_size_ << " write_trace_size_: " << write_trace_size_ << std::endl;
      read_key_trace_->Sort();
   }

   void FilterStepReadTrace(ThreadState* thread)
   {
      auto step = 24;
      auto start = 0;
      auto end = read_key_trace_->keys_.size();
      auto i = start;
      for (; (start + i * step) < end; i++) {
         read_key_trace_->keys_[i] = read_key_trace_->keys_[start + i * step];
      }
      read_key_trace_->keys_.erase(read_key_trace_->keys_.begin() + i, read_key_trace_->keys_.end());
      read_key_trace_->count_ = read_key_trace_->keys_.size();
      read_trace_size_ = read_key_trace_->count_;
      std::cout << "final state read_trace_size_: " << read_trace_size_ << std::endl;
   }

   void DoFastTrain(ThreadState* thread)
   {
      auto& table = *adapter;
      table.fast_train(FLAGS_max_error);
   }

   void DoTrain(ThreadState* thread)
   {
      auto& table = *adapter;
      table.train(FLAGS_max_error);
   }

   void DoRead(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoRead lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = read_key_trace_->trace_at(start_offset, read_trace_size_);
      size_t not_find = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t ikey = key_iterator.Next();
            YCSBPayload result;
            auto ret = table.lookup(ikey, result);

            if (!ret) {
               not_find++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu)", reads_, not_find);
      thread->stats.AddMessage(buf);
   }

   // void DoReadAllRS(ThreadState* thread)
   // {
   //    std::cout << "ReadAll" << std::endl;
   //    uint64_t batch = FLAGS_batch;
   //    if (read_key_trace_ == nullptr || read_trace_size_ == 0) {
   //       perror("DoReadAll lack key_trace_ initialization.");
   //       return;
   //    }
   //    read_trace_size_ = read_key_trace_->keys_.size();
   //    size_t interval = read_trace_size_ / FLAGS_worker_threads;
   //    size_t start_offset = thread->tid * interval;
   //    auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
   //    auto keys = read_key_trace_->keys_;
   //    std::sort(keys.begin(), keys.end());
   //    size_t not_find = 0;
   //    size_t found = 0;
   //    Duration duration(FLAGS_readtime, reads_);
   //    thread->stats.Start();
   //    while (!duration.Done(batch) && key_iterator.Valid()) {
   //       uint64_t j = 0;
   //       for (; j < batch && key_iterator.Valid(); j++) {
   //          auto ikey = key_iterator.Next();
   //          YCSBPayload result;
   //          auto ret = rsindex.find(ikey, keys);

   //          if (keys[ret] == ikey) {
   //             found++;
   //          } else {
   //             not_find++;
   //          }
   //       }
   //       thread->stats.FinishedBatchOp(j);
   //    }
   //    char buf[100];
   //    snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", interval, not_find, found);
   //    if (not_find)
   //       printf("thread %2d num: %lu, not find: %lu found: %lu\n", thread->tid, interval, not_find, found);
   //    thread->stats.AddMessage(buf);
   // }

   void DoReadUseSegmentAll(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoReadUseSegmentAll lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();

      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);

      size_t not_find = 0;
      size_t found = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;

      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t ikey = key_iterator.Next();
            // YCSBPayload result;
            // auto ret = table.trained_lookup(ikey, result);
            size_t value_length = 0;
            const u8* value_ptr = nullptr;
            auto ret = btree_ptr->fast_trained_lookup_new(ikey, [&](const u8* payload, u16 payload_length) {
               value_ptr = payload;
               value_length = payload_length;
               // return true;
            }) == OP_RESULT::OK;
            if (!ret) {
               not_find++;
            } else {
               found++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", interval, not_find, found);
      if (not_find)
         printf("thread %2d num: %lu, not find: %lu\n found: %lu", thread->tid, interval, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadUseSegmentZipf(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoReadUseSegmentAll lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();

      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto reads = (reads_ == 0) ? read_trace_size_ : reads_;
      reads = reads / FLAGS_worker_threads;
      // auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto key_iterator = read_key_trace_->zipfiterator(reads, FLAGS_zipfian_constant);
      size_t total = 0;
      size_t not_find = 0;
      size_t found = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t ikey = key_iterator.Next();
            YCSBPayload result;
            auto ret = table.trained_lookup(ikey, result);

            if (!ret) {
               not_find++;
            } else {
               found++;
            }
            total++;
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", interval, not_find, found);
      if (not_find)
         printf("thread %2d num: %lu, not find: %lu\n found: %lu", thread->tid, interval, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadAll(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr || read_trace_size_ == 0) {
         perror("DoReadAll lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
      size_t not_find = 0;
      size_t found = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            auto ikey = key_iterator.Next();
            YCSBPayload result;
            auto ret = table.lookup(ikey, result);

            if (!ret) {
               not_find++;
            } else {
               found++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", interval, not_find, found);
      if (not_find)
         printf("thread %2d num: %lu, not find: %lu found: %lu\n", thread->tid, interval, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadZipf(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr || read_trace_size_ == 0) {
         perror("DoReadZip lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->zipfiterator(interval, FLAGS_zipfian_constant);
      size_t not_find = 0;
      size_t found = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto reads = (reads_ == 0) ? read_trace_size_ : reads_;
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            auto ikey = key_iterator.Next();
            YCSBPayload result;
            auto ret = table.lookup(ikey, result);

            if (!ret) {
               not_find++;
            } else {
               found++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      std::cout << key_iterator.Info() << std::endl;
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", interval, not_find, found);
      if (not_find)
         printf("thread %2d num: %lu, not find: %lu found: %lu\n", thread->tid, interval, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadNon(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoRead lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = write_key_trace_->trace_at(start_offset, write_trace_size_);
      size_t not_find = 0, find = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            // size_t ikey = key_iterator.Next() + num_;
            auto key = key_iterator.Next();
            YCSBPayload result;

            // auto ret = table.lookup(ikey, result);
            auto ret = table.lookup(key, result);
            if (!ret) {
               not_find++;
            } else {
               find++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu find: %lu)", reads_, not_find, find);
      thread->stats.AddMessage(buf);
   }

   void DoReadLatWithSeg(ThreadState* thread)
   {
      if (read_key_trace_ == nullptr) {
         perror("DoReadLatWithSeg lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = read_key_trace_->trace_at(start_offset, read_trace_size_);

      size_t not_find = 0;
      size_t found = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.trained_lookup(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadFtailLat(ThreadState* thread)
   {
      std::cout << "DoReadFtailLat" << std::endl;
      if (read_key_trace_ == nullptr) {
         perror("DoReadFtailLat lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = read_key_trace_->trace_at(start_offset, read_trace_size_);
      size_t not_find = 0;
      size_t found = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.fast_tail_lookup(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoSimulateReadLat(ThreadState* thread)
   {
      std::cout << "DoSimulateReadLat" << std::endl;
      if (read_key_trace_ == nullptr) {
         perror("DoReadLat lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = read_key_trace_->trace_at(start_offset, read_trace_size_);
      size_t not_find = 0;
      size_t found = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.lookup_simulate_long_tail(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadZipfianLatWithSeg(ThreadState* thread)
   {
      // std::cout << "DoReadZipfianLat" << std::endl;
      if (read_key_trace_ == nullptr) {
         perror("DoReadZipfianLat lack key_trace_ initialization.");
         return;
      }
      auto reads = (reads_ == 0) ? read_key_trace_->keys_.size() : reads_;
      reads = reads / FLAGS_worker_threads;
      auto key_iterator = read_key_trace_->zipfiterator(reads, FLAGS_zipfian_constant);
      size_t not_find = 0;
      size_t found = 0;
      size_t total = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         // std::cout << "found: " << found << std::endl;
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.trained_lookup(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
         total++;
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }
   void DoReadZipfianLat(ThreadState* thread)
   {
      // std::cout << "DoReadZipfianLat" << std::endl;
      if (read_key_trace_ == nullptr) {
         perror("DoReadZipfianLat lack key_trace_ initialization.");
         return;
      }
      auto reads = (reads_ == 0) ? read_key_trace_->keys_.size() : reads_;
      reads = reads / FLAGS_worker_threads;
      auto key_iterator = read_key_trace_->zipfiterator(reads, FLAGS_zipfian_constant);
      size_t not_find = 0;
      size_t found = 0;
      size_t total = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         // std::cout << "found: " << found << std::endl;
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.lookup(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
         total++;
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadLat(ThreadState* thread)
   {
      std::cout << "DoReadLat" << std::endl;
      if (read_key_trace_ == nullptr) {
         perror("DoReadLat lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = read_key_trace_->trace_at(start_offset, read_trace_size_);
      size_t not_find = 0;
      size_t found = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         size_t ikey = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.lookup(ikey, result);
         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            found++;
         }
#ifdef DUMP_EACH_LATENCY
         std::cout << "latency: " << time_duration << " key: " << ikey << std::endl;
#endif
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu found: %lu)", reads_, not_find, found);
      thread->stats.AddMessage(buf);
   }

   void DoReadNonLat(ThreadState* thread)
   {
      if (read_key_trace_ == nullptr) {
         perror("DoReadLat lack key_trace_ initialization.");
         return;
      }
      read_trace_size_ = read_key_trace_->keys_.size();
      size_t start_offset = random() % read_trace_size_;
      auto key_iterator = write_key_trace_->trace_at(start_offset, write_trace_size_);
      size_t not_find = 0, find = 0;

      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(1) && key_iterator.Valid()) {
         // size_t ikey = key_iterator.Next() + num_;
         auto key = key_iterator.Next();

         auto time_start = NowNanos();
         YCSBPayload result;
         auto ret = table.lookup(key, result);
         // auto ret = table.lookup(ikey, result);

         auto time_duration = NowNanos() - time_start;

         thread->stats.hist_.Add(time_duration);
         if (!ret) {
            not_find++;
         } else {
            find++;
         }
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu)", reads_, not_find);
      thread->stats.AddMessage(buf);
   }
   // write data to leanstore
   void DoWriteFast(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("DoWrite lack key_trace_ initialization.");
         return;
      }
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();
      std::string val(value_size_, 'v');
      size_t inserted = 0;
      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            inserted++;
            YCSBPayload payload;
            // size_t ikey = key_iterator.Next();
            auto key = key_iterator.Next();
            // std::cout << "Inserted Key: " << key << std::endl;
            // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
            table.fast_insert(key, payload);
         }
         thread->stats.FinishedBatchOp(j);
      }
      return;
   }
   // write data to leanstore
   void DoWrite(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("DoWrite lack key_trace_ initialization.");
         return;
      }
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();
      std::string val(value_size_, 'v');
      size_t inserted = 0;
      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            inserted++;
            YCSBPayload payload;
            // size_t ikey = key_iterator.Next();
            auto key = key_iterator.Next();
            // std::cout << "Inserted Key: " << key << std::endl;
            // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
            table.insert(key, payload);
         }
         thread->stats.FinishedBatchOp(j);
      }
      return;
   }

   void DoWriteLat(ThreadState* thread)
   {
      if (write_key_trace_ == nullptr) {
         perror("DoWriteLat lack key_trace_ initialization.");
         return;
      }
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();
      while (key_iterator.Valid()) {
         // size_t ikey = key_iterator.Next();
         auto key = key_iterator.Next();
         auto time_start = NowNanos();
         YCSBPayload payload;
         // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
         table.insert(key, payload);
         // table.insert(ikey, payload);
         auto time_duration = NowNanos() - time_start;
         thread->stats.hist_.Add(time_duration);
#ifdef DUMP_EACH_LATENCY
         std::cout << "Latency: " << time_duration << " key: " << key << std::endl;
#endif
      }
      return;
   }

   void DoOverWrite(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("DoOverWrite lack key_trace_ initialization.");
         return;
      }
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();
      std::string val(value_size_, 'v');
      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            // size_t ikey = key_iterator.Next();
            auto key = key_iterator.Next();
            YCSBPayload payload;
            // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
            table.insert(key, payload);
         }
         thread->stats.FinishedBatchOp(j);
      }
      return;
   }

   void YCSBA_Seg(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("YCSBA lack key_trace_ initialization.");
         return;
      }
      size_t find = 0, not_find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
#ifdef YCSB_USE_READ_TRACE
      auto read_key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
#endif
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            YCSBPayload payload;
            if (thread->ycsb_gen.NextA() == kYCSB_Write) {
               YCSBKey key = key_iterator.Next();
// utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
#ifdef SEG_IN_INSERT
               table.fast_insert(key, payload);
#else
               table.insert(key, payload);
#endif
               insert++;
            } else {
#ifdef YCSB_USE_READ_TRACE
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (start_offset + insert)) {
                  // YCSBKey key = read_key_iterator.Next();
                  YCSBKey key = read_key_trace_->keys_[ikey];
                  auto found = table.trained_lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#else
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (start_offset + interval)) {
                  YCSBKey key = write_key_trace_->keys_[ikey];
                  auto found = table.fast_lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#endif
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu, found: %lu, notfound: %lu)", insert, find + not_find, find, not_find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBA(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("YCSBA lack key_trace_ initialization.");
         return;
      }
      size_t find = 0, not_find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
#ifdef YCSB_USE_READ_TRACE
      auto read_key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
#endif
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            YCSBPayload payload;
            if (thread->ycsb_gen.NextA() == kYCSB_Write) {
               YCSBKey key = key_iterator.Next();
               // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               table.insert(key, payload);
               insert++;
            } else {
#ifdef YCSB_USE_READ_TRACE
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (start_offset + insert)) {
                  // YCSBKey key = read_key_iterator.Next();
                  YCSBKey key = read_key_trace_->keys_[ikey];
                  auto found = table.lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#else
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (insert + start_offset)) {
                  YCSBKey key = write_key_trace_->keys_[ikey];
                  auto found = table.lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#endif
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu found: %lu notfound: %lu)", insert, find + not_find, find, not_find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBB_Seg(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("YCSBB lack key_trace_ initialization.");
         return;
      }
      size_t find = 0, not_find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
#ifdef YCSB_USE_READ_TRACE
      auto read_key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
#endif
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid() && read_key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid() && read_key_iterator.Valid(); j++) {
            YCSBPayload payload;
            if (thread->ycsb_gen.NextB() == kYCSB_Write) {
               YCSBKey key = key_iterator.Next();
// utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
#ifdef SEG_IN_INSERT
               table.fast_insert(key, payload);
#else
               table.insert(key, payload);
#endif
               insert++;
            } else {
#ifdef YCSB_USE_READ_TRACE
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (start_offset + insert)) {
                  // YCSBKey key = read_key_iterator.Next();
                  YCSBKey key = read_key_trace_->keys_[ikey];
                  auto found = table.trained_lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#else
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (insert + start_offset)) {
                  YCSBKey key = write_key_trace_->keys_[ikey];
                  auto found = table.fast_lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#endif
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu, found: %lu, not_found: %lu)", insert, find + not_find, find, not_find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBB(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("YCSBB lack key_trace_ initialization.");
         return;
      }
      size_t find = 0, not_find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
#ifdef YCSB_USE_READ_TRACE
      auto read_key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
#endif
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            YCSBPayload payload;
            if (thread->ycsb_gen.NextB() == kYCSB_Write) {
               YCSBKey key = key_iterator.Next();
               // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               table.insert(key, payload);
               insert++;
            } else {
#ifdef YCSB_USE_READ_TRACE
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (start_offset + insert)) {
                  // YCSBKey key = read_key_iterator.Next();
                  YCSBKey key = read_key_trace_->keys_[ikey];
                  auto found = table.lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#else
               size_t ikey = rand() / static_cast<float>(RAND_MAX) * insert + start_offset;
               if (ikey < (insert + start_offset)) {
                  YCSBKey key = write_key_trace_->keys_[ikey];
                  auto found = table.lookup(key, payload);
                  if (found) {
                     find++;
                  } else {
                     not_find++;
                  }
               }
#endif
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu, found: %lu, not_found: %lu)", insert, find + not_find, find, not_find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBC(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("YCSBC lack key_trace_ initialization.");
         return;
      }
      size_t find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t key = key_iterator.Next();
            YCSBPayload payload;
            auto ret = table.lookup(key, payload);
            if (ret) {
               find++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu)", insert, find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBD(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("YCSBD lack key_trace_ initialization.");
         return;
      }
      size_t find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      // Read the latest 20%
      auto key_iterator = read_key_trace_->iterate_between(start_offset + 0.8 * interval, start_offset + interval);
      printf("thread %2d, between %lu - %lu\n", thread->tid, (size_t)(start_offset + 0.8 * interval), start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t key = key_iterator.Next();
            YCSBPayload payload;
            auto ret = table.lookup(key, payload);

            if (ret) {
               find++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(insert: %lu, read: %lu)", insert, find);
      thread->stats.AddMessage(buf);
      return;
   }

   void YCSBF(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (write_key_trace_ == nullptr) {
         perror("YCSBF lack key_trace_ initialization.");
         return;
      }
      size_t find = 0;
      size_t insert = 0;
      read_trace_size_ = read_key_trace_->keys_.size();
      write_trace_size_ = write_key_trace_->keys_.size();
      size_t interval = write_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = write_key_trace_->iterate_between(start_offset, start_offset + interval);
      auto& table = *adapter;
      thread->stats.Start();

      while (key_iterator.Valid()) {
         uint64_t j = 0;
         for (; j < batch && key_iterator.Valid(); j++) {
            size_t key = key_iterator.Next();
            YCSBPayload payload;
            if (thread->ycsb_gen.NextF() == kYCSB_Read) {
               auto ret = table.lookup(key, payload);

               if (ret) {
                  find++;
               }
            } else {
               table.lookup(key, payload);
               // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               table.insert(key, payload);
               insert++;
            }
         }
         thread->stats.FinishedBatchOp(j);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(read_modify: %lu, read: %lu)", insert, find);
      thread->stats.AddMessage(buf);
      return;
   }

   void Statistics(ThreadState* thread)
   {
      printf("================ Leanstore Statistics =================\n");
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      auto& table = *adapter;
      table.stats();
   }

   void DoPoolStats(ThreadState* thread)
   {
      printf("================ Leanstore Statistics =================\n");
      db.getBufferManager().BufferPoolUseInf();
   }

   void dumpTrace()
   {
      read_key_trace_->Sort();
      for (auto i = 0ul; i < read_key_trace_->keys_.size(); i++) {
         std::cout << "[key_trace] index:" << i << " key: " << read_key_trace_->keys_[i] << std::endl;
      }
      write_key_trace_->Sort();
      for (auto i = 0ul; i < write_key_trace_->keys_.size(); i++) {
         std::cout << "[key_trace] index:" << i << " key: " << write_key_trace_->keys_[i] << std::endl;
      }
   }

   void DoSaveTrace(ThreadState* thread)
   {
      printf("================ Save Trace =================\n");
      auto starttime = std::chrono::system_clock::now();
      auto filepath = FLAGS_tracefile;
      if (filepath.compare(filepath.size() - 4, 4, ".csv") == 0) {
         read_key_trace_->ToCSV(filepath);
      } else {
         read_key_trace_->ToFile(filepath);
      }
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
      printf("save trace time: %f s.\n", duration.count() / 1000000.0);
      if constexpr (TRACE_DUMP) {
         dumpTrace();
      }
      printf("================ Save Trace =================\n");
   }

   void DoReadTraceLoad(ThreadState* thread)
   {
      printf("================ [DoReadTraceLoad] Begin =================\n");
      auto starttime = std::chrono::system_clock::now();
      auto filepath = FLAGS_tracefile;
      std::cout << "filepath: " << filepath << std::endl;
      if (filepath.compare(filepath.size() - 4, 4, ".csv") == 0) {
         std::cout << "[DoReadTraceLoad] Load trace from csv file: " << filepath << std::endl;
         read_key_trace_->FromCSV(filepath);
      } else {
         std::cout << "[DoReadTraceLoad] Load trace from file: " << filepath << std::endl;
#ifdef TRACE_SOSD
         read_key_trace_->FromSOSD(filepath);
#else
         read_key_trace_->FromFile(filepath);
#endif
      }
      // key_trace_->FromFile(FLAGS_tracefile);
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
      read_key_trace_->RemoveDuplicates();
      // num_ = read_key_trace_->count_;
      read_trace_size_ = read_key_trace_->keys_.size();
      printf("[DoReadTraceLoad] read trace time: %f s. trace size: %lu \n", duration.count() / 1000000.0, read_trace_size_);
      if constexpr (TRACE_DUMP) {
         dumpTrace();
      }
      printf("================ [DoReadTraceLoad] End =================\n");
   }
   void DoWriteTraceLoad(ThreadState* thread)
   {
      printf("================ [DoWriteTraceLoad] Begin =================\n");
      auto starttime = std::chrono::system_clock::now();
      auto filepath = FLAGS_tracefile;
      std::cout << "[DoWriteTraceLoad] filepath: " << filepath << std::endl;
      if (filepath.compare(filepath.size() - 4, 4, ".csv") == 0) {
         std::cout << "[DoWriteTraceLoad] Load trace from csv file: " << filepath << std::endl;
         write_key_trace_->FromCSV(filepath);
      } else {
         std::cout << "[DoWriteTraceLoad] Load trace from file: " << filepath << std::endl;
#ifdef TRACE_SOSD
         write_key_trace_->FromSOSD(filepath);
#else
         write_key_trace_->FromFile(filepath);
#endif
      }
      // key_trace_->FromFile(FLAGS_tracefile);
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
      write_key_trace_->RemoveDuplicates();
      // num_ = read_key_trace_->count_;
      write_trace_size_ = write_key_trace_->keys_.size();
      printf("[DoWriteTraceLoad] write trace time: %f s. trace size: %lud \n", duration.count() / 1000000.0, read_trace_size_);
      if constexpr (TRACE_DUMP) {
         dumpTrace();
      }
      printf("================ [DoWriteTraceLoad] End =================\n");
   }

   void DoScanAllAsc(ThreadState* thread)
   {
      auto& table = *adapter;
      table.scan_asc_all();
   }

   void DoScanAllAscSeg(ThreadState* thread)
   {
      auto& table = *adapter;
      table.scan_asc_all_seg();
   }

   void DoScanAsc(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoScanAsc lack read_key_trace_ initialization.");
         return;
      }

      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);

      size_t num_not_find = 0;
      size_t num_find = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      auto range = FLAGS_range_length;
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t count = 0;
         for (; count < batch && key_iterator.Valid(); ++count) {
            size_t key = key_iterator.Next();
            YCSBPayload value;
            bool result = table.scan_asc(key, range);
            if (result) {
               num_find++;
            } else {
               num_not_find++;
            }
         }
         thread->stats.FinishedBatchOp(count);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu, find: %lu)", interval, num_not_find, num_find);
      if (num_not_find) {
         printf("thread %2d num: %lu, not find: %lu; find: %lu\n", thread->tid, interval, num_not_find, num_find);
      }
      thread->stats.AddMessage(buf);
   }

   void DoScanDesc(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoScanDesc lack read_key_trace_ initialization.");
         return;
      }

      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);

      size_t num_not_find = 0;
      size_t num_find = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t count = 0;
         for (; count < batch && key_iterator.Valid(); ++count) {
            size_t key = key_iterator.Next();
            YCSBPayload value;
            bool result = table.scan_desc(key, value);

            if (result) {
               num_find++;
            } else {
               num_not_find++;
            }
         }
         thread->stats.FinishedBatchOp(count);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu, find: %lu)", interval, num_not_find, num_find);
      if (num_not_find) {
         printf("thread %2d num: %lu, not find: %lu; find: %lu\n", thread->tid, interval, num_not_find, num_find);
      }
      thread->stats.AddMessage(buf);
   }

   void DoScanAscSeg(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoScanAscSeg lack read_key_trace_ initialization.");
         return;
      }

      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);

      size_t num_not_find = 0;
      size_t num_find = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      auto range = FLAGS_range_length;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t count = 0;
         for (; count < batch && key_iterator.Valid(); ++count) {
            size_t key = key_iterator.Next();
            YCSBPayload value;
            bool result = table.scan_asc_seg(key, range);

            if (result) {
               num_find++;
            } else {
               num_not_find++;
            }
         }
         thread->stats.FinishedBatchOp(count);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu, find: %lu)", interval, num_not_find, num_find);
      if (num_not_find) {
         printf("thread %2d num: %lu, not find: %lu; find: %lu\n", thread->tid, interval, num_not_find, num_find);
      }
      thread->stats.AddMessage(buf);
   }

   void DoScanDescSeg(ThreadState* thread)
   {
      uint64_t batch = FLAGS_batch;
      if (read_key_trace_ == nullptr) {
         perror("DoScanDescSeg lack read_key_trace_ initialization.");
         return;
      }

      read_trace_size_ = read_key_trace_->keys_.size();
      size_t interval = read_trace_size_ / FLAGS_worker_threads;
      size_t start_offset = thread->tid * interval;
      auto key_iterator = read_key_trace_->iterate_between(start_offset, start_offset + interval);

      size_t num_not_find = 0;
      size_t num_find = 0;
      Duration duration(FLAGS_readtime, reads_);
      auto& table = *adapter;
      thread->stats.Start();
      while (!duration.Done(batch) && key_iterator.Valid()) {
         uint64_t count = 0;
         for (; count < batch && key_iterator.Valid(); ++count) {
            size_t key = key_iterator.Next();
            YCSBPayload value;
            bool result = table.scan_desc_seg(key, value);

            if (result) {
               num_find++;
            } else {
               num_not_find++;
            }
         }
         thread->stats.FinishedBatchOp(count);
      }
      char buf[100];
      snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu, find: %lu)", interval, num_not_find, num_find);
      if (num_not_find) {
         printf("thread %2d num: %lu, not find: %lu; find: %lu\n", thread->tid, interval, num_not_find, num_find);
      }
      thread->stats.AddMessage(buf);
   }

  private:
   struct ThreadArg {
      Benchmark* bm;
      SharedState* shared;
      ThreadState* thread;
      void (Benchmark::*method)(ThreadState*);
   };

   static void ThreadBody(void* v)
   {
      ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
      SharedState* shared = arg->shared;
      ThreadState* thread = arg->thread;
      {
         std::unique_lock<std::mutex> lck(shared->mu);
         shared->num_initialized++;
         if (shared->num_initialized >= shared->total) {
            shared->cv.notify_all();
         }
         while (!shared->start) {
            shared->cv.wait(lck);
         }
      }

      thread->stats.Start();
      (arg->bm->*(arg->method))(thread);
      thread->stats.Stop();

      {
         std::unique_lock<std::mutex> lck(shared->mu);
         shared->num_done++;
         if (shared->num_done >= shared->total) {
            shared->cv.notify_all();
         }
      }
   }

   void RunBenchmark(int thread_num, const std::string& name, void (Benchmark::*method)(ThreadState*), bool print_hist)
   {
      SharedState shared(thread_num);
      ThreadArg* arg = new ThreadArg[thread_num];
      std::thread server_threads[thread_num];
      for (int i = 0; i < thread_num; i++) {
         arg[i].bm = this;
         arg[i].method = method;
         arg[i].shared = &shared;
         arg[i].thread = new ThreadState(i);
         arg[i].thread->shared = &shared;
         server_threads[i] = std::thread(ThreadBody, &arg[i]);
      }

      std::unique_lock<std::mutex> lck(shared.mu);
      while (shared.num_initialized < thread_num) {
         shared.cv.wait(lck);
      }

      shared.start = true;
      shared.cv.notify_all();
      while (shared.num_done < thread_num) {
         shared.cv.wait(lck);
      }

      for (int i = 1; i < thread_num; i++) {
         arg[0].thread->stats.Merge(arg[i].thread->stats);
      }
      arg[0].thread->stats.Report(name, print_hist);

      for (auto& th : server_threads)
         th.join();
   }

   void PrintEnvironment()
   {
#if defined(__linux)
      time_t now = time(nullptr);
      fprintf(stderr, "Date:                  %s", ctime(&now));  // ctime() adds newline

      FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
      if (cpuinfo != nullptr) {
         char line[1000];
         int num_cpus = 0;
         std::string cpu_type;
         std::string cache_size;
         while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
            const char* sep = strchr(line, ':');
            if (sep == nullptr) {
               continue;
            }
            std::string key = TrimSpace(std::string(line, sep - 1 - line));
            std::string val = TrimSpace(std::string(sep + 1));
            if (key == "model name") {
               ++num_cpus;
               cpu_type = val;
            } else if (key == "cache size") {
               cache_size = val;
            }
         }
         fclose(cpuinfo);
         fprintf(stderr, "CPU:                   %d * %s\n", num_cpus, cpu_type.c_str());
         fprintf(stderr, "CPUCache:              %s\n", cache_size.c_str());
      }
#endif
   }

   void PrintHeader()
   {
      fprintf(stdout, "------------------------------------------------\n");
      PrintEnvironment();
      fprintf(stdout, "BenchType:             %s\n", "Leanstore-LearnedIndex");
      // fprintf(stdout, "Entries:               %lu\n", (uint64_t)num_);
      fprintf(stdout, "Key Size:              %lu\n", (uint64_t)sizeof(YCSBKey));
      fprintf(stdout, "Value Size:            %lu\n", (uint64_t)sizeof(YCSBPayload));
      fprintf(stdout, "Read Trace size:       %lu\n", (uint64_t)read_trace_size_);
      fprintf(stdout, "Write Trace size:      %lu\n", (uint64_t)write_trace_size_);
      fprintf(stdout, "Read:                  %lu\n", (uint64_t)FLAGS_read);
      fprintf(stdout, "Write:                 %lu\n", (uint64_t)FLAGS_write);
      fprintf(stdout, "Thread:                %lu\n", (uint64_t)FLAGS_worker_threads);
      fprintf(stdout, "Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
      fprintf(stdout, "Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
      fprintf(stdout, "benchmarks:            %s\n", FLAGS_benchmarks.c_str());
      fprintf(stdout, "------------------------------------------------\n");
   }
};

int main(int argc, char* argv[])
{
   srand(0);
   for (int i = 0; i < argc; i++) {
      printf("%s ", argv[i]);
   }
   printf("\n");

   ParseCommandLineFlags(&argc, &argv, true);

   // tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
   tbb::global_control control(tbb::global_control::max_allowed_parallelism, FLAGS_worker_threads);

   Benchmark benchmark;
   benchmark.Run();
   return 0;
}
