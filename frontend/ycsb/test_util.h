#pragma once

#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/time.h>
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_sort.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <memory>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "slice.h"
#include "zipfian_int_distribution.h"
#define TRACE_WITH_SIZE
// #define TRACE_SOSD
#define KEY_LEN ((15))

auto rng = std::default_random_engine{};

static constexpr uint64_t kRandNumMax = (1LU << 48);
static constexpr uint64_t kRandNumMaxMask = kRandNumMax - 1;

static uint64_t u64Rand(const uint64_t& min, const uint64_t& max)
{
   static thread_local std::mt19937 generator(std::random_device{}());
   std::uniform_int_distribution<uint64_t> distribution(min, max);
   return distribution(generator);
}

namespace util
{

// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random
{
  private:
   uint32_t seed_;

  public:
   Random() { Random(rand()); }
   explicit Random(uint32_t s) : seed_(s & 0x7fffffffu)
   {
      // Avoid bad seeds.
      if (seed_ == 0 || seed_ == 2147483647L) {
         seed_ = 1;
      }
   }
   uint32_t Next()
   {
      static const uint32_t M = 2147483647L;  // 2^31-1
      static const uint64_t A = 16807;        // bits 14, 8, 7, 5, 2, 1, 0
      // We are computing
      //       seed_ = (seed_ * A) % M,    where M = 2^31-1
      //
      // seed_ must not be zero or M, or else all subsequent computed values
      // will be zero or M respectively.  For all other values, seed_ will end
      // up cycling through every number in [1,M-1]
      uint64_t product = seed_ * A;

      // Compute (product % M) using the fact that ((x << 31) % M) == x.
      seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
      // The first reduction may overflow by 1 bit, so we may need to
      // repeat.  mod == M is not possible; using > allows the faster
      // sign-bit-based test.
      if (seed_ > M) {
         seed_ -= M;
      }
      return seed_;
   }
   // Returns a uniformly distributed value in the range [0..n-1]
   // REQUIRES: n > 0
   uint32_t Uniform(int n) { return Next() % n; }

   // Randomly returns true ~"1/n" of the time, and false otherwise.
   // REQUIRES: n > 0
   bool OneIn(int n) { return (Next() % n) == 0; }

   // Skewed: pick "base" uniformly from range [0,max_log] and then
   // return "base" random bits.  The effect is to pick a number in the
   // range [0,2^max_log-1] with exponential bias towards smaller numbers.
   uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
};

Slice RandomString(Random* rnd, int len, std::string* dst)
{
   dst->resize(len);
   for (int i = 0; i < len; i++) {
      (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
   }
   return Slice(*dst);
}

// Helper for quickly generating random data.
class RandomGenerator
{
  private:
   std::string data_;
   int pos_;

  public:
   RandomGenerator()
   {
      // We use a limited amount of data over and over again and ensure
      // that it is larger than the compression window (32KB), and also
      // large enough to serve all typical value sizes we want to write.
      Random rnd(301);
      std::string piece;
      while (data_.size() < 1048576) {
         // Add a short fragment that is as compressible as specified
         // by FLAGS_compression_ratio.
         RandomString(&rnd, 100, &piece);
         data_.append(piece);
      }
      pos_ = 0;
   }

   Slice Generate(size_t len)
   {
      if (pos_ + len > data_.size()) {
         pos_ = 0;
         assert(len < data_.size());
      }
      pos_ += len;
      return Slice(data_.data() + pos_ - len, len);
   }
};

inline std::string Execute(const std::string& cmd)
{
   std::array<char, 128> buffer;
   std::string result;
   std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
   if (!pipe) {
      throw std::runtime_error("popen() failed!");
   }
   while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      result += buffer.data();
   }
   return result;
}

inline uint64_t NowMicros()
{
   static constexpr uint64_t kUsecondsPerSecond = 1000000;
   struct ::timeval tv;
   ::gettimeofday(&tv, nullptr);
   return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}
// Returns the number of nano-seconds since some fixed point in time. Only
// useful for computing deltas of time in one run.
// Default implementation simply relies on NowMicros.
// In platform-specific implementations, NowNanos() should return time points
// that are MONOTONIC.
inline uint64_t NowNanos()
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return static_cast<uint64_t>(ts.tv_sec) * 1000000000L + ts.tv_nsec;
}

template <typename key_t>
class KeyTrace
{
  public:
   KeyTrace() : count_(0){};
   void generate(size_t count, key_t step = 0, KeyTrace<key_t>* read_trace = nullptr, key_t start_offset = 0)
   {
      count_ = count;
      keys_.reserve(count);
      printf("generating %lu keys\n", count);
      if (read_trace != nullptr) {
         read_trace->Sort();
         if (step != 0 && read_trace->keys_.size() > 0)
            start_offset = read_trace->keys_[read_trace->keys_.size() - 1];
      }
      auto starttime = std::chrono::system_clock::now();

      // tbb::parallel_for(tbb::blocked_range<uint64_t>(0, count), [&](const tbb::blocked_range<uint64_t>& range) {
      //    for (uint64_t i = range.begin(); i != range.end(); i++) {
      //       if (isSeq) {
      //          if (read_trace != nullptr) {
      //             auto isfound = find(read_trace->keys_.begin(), read_trace->keys_.end(), i + 1);
      //             if (isfound == read_trace->keys_.end())
      //                // keys_[i] = i + 1;
      //                keys_.push_back(i + 1);

      //          } else {
      //             keys_.push_back(i + 1);
      //          }
      //       } else {
      //          uint64_t num = u64Rand(1LU, kRandNumMax);
      //          if (read_trace != nullptr) {
      //             auto isfound = find(read_trace->keys_.begin(), read_trace->keys_.end(), num);
      //             if (isfound == read_trace->keys_.end())
      //                keys_.push_back(num);

      //          } else {
      //             keys_.push_back(num);
      //          }
      //       }
      //    }
      // });
      if (step > 0) {
         printf("generating sequential keys with step %lu and offset %lu\n", step, start_offset);
         for (uint64_t i = 0; i != count; i++) {
            auto ikey = (i + 1) * step;
            if (read_trace != nullptr) {
               auto isfound = lower_bound(read_trace->keys_.begin(), read_trace->keys_.end(), ikey);
               if (isfound == read_trace->keys_.end()) {
                  keys_.push_back(ikey);
               } else if (*isfound != (ikey)) {
                  keys_.push_back(ikey);
               }
            } else {
               keys_.push_back(ikey);
            }
         }
      } else {
         printf("generating random keys\n");
         for (uint64_t i = 0; i != count; i++) {
            uint64_t num = u64Rand(1LU, kRandNumMax);
            if (read_trace != nullptr) {
               auto isfound = lower_bound(read_trace->keys_.begin(), read_trace->keys_.end(), num);
               if (isfound == read_trace->keys_.end()) {
                  keys_.push_back(num);
               } else if (*isfound != num) {
                  keys_.push_back(num);
               }

            } else {
               keys_.push_back(num);
            }
         }
      }
      printf("Ensure there is no duplicate keys\n");
      RemoveDuplicates();
      printf("%lu keys left \n", keys_.size());
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
      printf("generate duration %f s.\n", duration.count() / 1000000.0);
      // Randomize();
   }

   ~KeyTrace() {}

   void ToCSV(const std::string& filename)
   {
      std::ofstream outfile(filename, std::ios::out);
      for (auto k : keys_) {
         outfile << k << std::endl;
      }
      outfile.close();
   }

   void FromCSV(const std::string& filename)
   {
      std::ifstream infile(filename, std::ios::in);
      if (!infile.is_open()) {
         printf("open file %s failed\n", filename.c_str());
         return;
      }
      std::string line;
      while (std::getline(infile, line)) {
         std::istringstream iss(line);
         key_t data;
         iss >> data;
         keys_.push_back(data);
      }
      infile.close();
      count_ = keys_.size();
   }

   void ToFile(const std::string& filename)
   {
      std::ofstream outfile(filename, std::ios::out | std::ios_base::binary);
#ifdef TRACE_WITH_SIZE
      size_t size = keys_.size();
      outfile.write(reinterpret_cast<char*>(&size), sizeof(size));
#endif
      for (auto k : keys_) {
         outfile.write(reinterpret_cast<char*>(&k), sizeof(k));
         // printf ("out key %016lu\n", k);
      }
      outfile.close();
   }

   void FromFile(const std::string& filename)
   {
      std::ifstream infile(filename, std::ios::in | std::ios_base::binary);
      if (!infile.is_open()) {
         std::cout << "[FromFile] Error opening " << filename << std::endl;
         return;
      }
      key_t key;
      keys_.clear();
#ifdef TRACE_WITH_SIZE
      infile.read(reinterpret_cast<char*>(&count_), 8);
      keys_.reserve(count_);
#endif
      while (!infile.eof()) {
         infile.read(reinterpret_cast<char*>(&key), sizeof(key));
         keys_.push_back(key);
         // printf (" in key %016lu\n", key);
      }
      count_ = keys_.size();
      infile.close();
   }

   void FromSOSD(const std::string& filename)
   {
      std::ifstream in(filename, std::ios::binary);
      if (!in.is_open()) {
         std::cerr << "Error opening file: " << filename << std::endl;
         exit(EXIT_FAILURE);
      }
      // Read size.
      uint64_t size;
      in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
      keys_.resize(size);
      // Read values.
      in.read(reinterpret_cast<char*>(keys_.data()), size * sizeof(key_t));
      in.close();
      count_ = keys_.size();
   }

   void Randomize(void)
   {
      printf("randomize %lu keys\n", keys_.size());
      auto starttime = std::chrono::system_clock::now();
      // tbb::parallel_for(tbb::blocked_range<uint64_t>(0, keys_.size()), [&](const tbb::blocked_range<uint64_t>& range) {
      //    auto rng = std::default_random_engine{};
      //    std::shuffle(keys_.begin() + range.begin(), keys_.begin() + range.end(), rng);
      // });
      auto rng = std::default_random_engine{};
      std::shuffle(keys_.begin(), keys_.end(), rng);
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
      printf("randomize duration %f s.\n", duration.count() / 1000000.0);
   }

   void Sort(void) { tbb::parallel_sort(keys_.begin(), keys_.end()); }

   void RemoveDuplicates(void)
   {
      Sort();
      auto it = std::unique(keys_.begin(), keys_.end());
      keys_.erase(it, keys_.end());
      count_ = keys_.size();
   }

   class RangeIterator
   {
     public:
      RangeIterator(std::vector<key_t>* pkey_vec, size_t start, size_t end) : pkey_vec_(pkey_vec), end_index_(end), cur_index_(start) {}

      inline bool Valid() { return (cur_index_ < end_index_); }

      inline key_t operator*() { return (*pkey_vec_)[cur_index_]; }

      inline key_t& Next() { return (*pkey_vec_)[cur_index_++]; }
      inline key_t* NextRef() { return &(*pkey_vec_)[cur_index_++]; }

      std::vector<key_t>* pkey_vec_;
      size_t end_index_;
      size_t cur_index_;
   };

   class Iterator
   {
     public:
      Iterator(std::vector<key_t>* pkey_vec, size_t start_index, size_t range)
          : pkey_vec_(pkey_vec), range_(range), end_index_(start_index % range_), cur_index_(start_index % range_), begin_(true)
      {
      }

      Iterator() {}

      inline bool Valid() { return (begin_ || cur_index_ != end_index_); }

      inline key_t& Next()
      {
         begin_ = false;
         size_t index = cur_index_;
         cur_index_++;
         if (cur_index_ >= range_) {
            cur_index_ = 0;
         }
         return (*pkey_vec_)[index];
      }

      inline size_t operator*() { return (*pkey_vec_)[cur_index_]; }

      std::string Info()
      {
         char buffer[128];
         sprintf(buffer, "valid: %s, cur i: %lu, end_i: %lu, range: %lu", Valid() ? "true" : "false", cur_index_, end_index_, range_);
         return buffer;
      }

      std::vector<key_t>* pkey_vec_;
      size_t range_;
      size_t end_index_;
      size_t cur_index_;
      bool begin_;
   };

   class ZipfIterator
   {
     public:
      ZipfIterator(std::vector<key_t>* pkey_vec, size_t num, double theta) : pkey_vec_(pkey_vec), num_(num), theta_(theta)
      {
         std::default_random_engine generator_;
         auto size = pkey_vec->size();
         zipfian_int_distribution<int> distribution_(0, size, theta_);
         index_.reserve(num_);
         // pkey_vec_->push_back(pkey_vec->at(size - 1));
         // pkey_vec_->push_back(pkey_vec->at(size - 1));
         // pkey_vec_->push_back(rand() % 100000);
         // pkey_vec_->push_back(rand() % 100000);
         for (size_t i = 0; i < num_; i++) {
            auto idx = distribution_(generator_);
            // if (idx < 0 || idx >= size) {
            //    std::cout << "idx: " << idx << " key: " << (*pkey_vec)[idx] << " out of range: " << pkey_vec->size() << std::endl;
            //    // exit(1);
            // } else {
            //    // std::cout << "idx: " << idx << " key: " << (*pkey_vec)[idx] << " range: " << pkey_vec->size() << std::endl;
            // }
            index_.push_back(idx);
         }
      }

      ~ZipfIterator()
      {
         // std::cout << Info() << std::endl;
         pkey_vec_->pop_back();
         pkey_vec_->pop_back();
      }

      inline key_t& Next() { return (*pkey_vec_)[index_[pos_++]]; }

      bool Valid() { return pos_ < num_; }

      std::string Info()
      {
         auto it = std::unique(index_.begin(), index_.end());
         char buffer[128];
         sprintf(buffer, "theta: %f unique_keys: %d", std::distance(index_.begin(), it));
         return buffer;
      }

      double theta_;
      std::vector<size_t> index_;
      std::vector<key_t>* pkey_vec_;
      size_t pos_ = 0;
      size_t num_ = 0;
   };

   Iterator trace_at(size_t start_index, size_t range) { return Iterator(&keys_, start_index, range); }

   RangeIterator Begin(void) { return RangeIterator(&keys_, 0, keys_.size()); }

   RangeIterator iterate_between(size_t start, size_t end) { return RangeIterator(&keys_, start, end); }
   ZipfIterator zipfiterator(size_t num, double theta) { return ZipfIterator(&keys_, num, theta); }

   size_t count_;
   std::vector<key_t> keys_;
};

enum YCSBOpType { kYCSB_Write, kYCSB_Read, kYCSB_Query, kYCSB_ReadModifyWrite };

inline uint32_t wyhash32()
{
   static thread_local uint32_t wyhash32_x = random();
   wyhash32_x += 0x60bee2bee120fc15;
   uint64_t tmp;
   tmp = (uint64_t)wyhash32_x * 0xa3b195354a39b70d;
   uint32_t m1 = (tmp >> 32) ^ tmp;
   tmp = (uint64_t)m1 * 0x1b03738712fad5c9;
   uint32_t m2 = (tmp >> 32) ^ tmp;
   return m2;
}

class YCSBGenerator
{
  public:
   // Generate
   YCSBGenerator() {}

   inline YCSBOpType NextA()
   {
      // ycsba: 50% reads, 50% writes
      uint32_t rnd_num = wyhash32();

      if ((rnd_num & 0x1) == 0) {
         return kYCSB_Read;
      } else {
         return kYCSB_Write;
      }
   }

   inline YCSBOpType NextB()
   {
      // ycsbb: 95% reads, 5% writes
      // 51/1024 = 0.0498
      uint32_t rnd_num = wyhash32();

      if ((rnd_num & 1023) < 51) {
         return kYCSB_Write;
      } else {
         return kYCSB_Read;
      }
   }

   inline YCSBOpType NextC() { return kYCSB_Read; }

   inline YCSBOpType NextD()
   {
      // ycsbd: read latest inserted records
      return kYCSB_Read;
   }

   inline YCSBOpType NextF()
   {
      // ycsba: 50% reads, 50% writes
      uint32_t rnd_num = wyhash32();

      if ((rnd_num & 0x1) == 0) {
         return kYCSB_Read;
      } else {
         return kYCSB_ReadModifyWrite;
      }
   }
};

}  // namespace util
