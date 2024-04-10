#pragma once
#include <stddef.h>
#include <algorithm>
#include <cmath>
#include <flat_hash_map.hpp>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <vector>
struct SearchBound {
   size_t begin;
   size_t end;  // Exclusive.
};
template <typename keytype>
class learnedindex
{
  public:
   learnedindex() : m(0), c(0), error(0) {}
   learnedindex(long double m, long double c, size_t error) : m(m), c(c), error(error) {}
   void load(long double m, long double c, size_t error, u64 ver)
   {
      this->m = m;
      this->c = c;
      this->error = error;
      version = true;
   }
   inline void train(const std::vector<keytype>& keys, const u64 version)
   {
      train(keys);
      this->version = version;
   }
   // train the learned index on the added keys
   void train(const std::vector<keytype>& keys)
   {
      auto n = keys.size();
      if (n <= 1) {
         m = 0;
         c = 0;
         error = 0;
         return;
      }
      // compute the least squares regression line y = mx + c
      long double sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
      for (auto i = 0ul; i < n; ++i) {
         // std::cout << " key: " << keys[i] << " index: " << i << std::endl;
         sum_x += keys[i];
         sum_y += i;
         sum_xy += keys[i] * i;
         sum_xx += keys[i] * keys[i];
      }
      long double delta = static_cast<long double>(n) * sum_xx - sum_x * sum_x;
      if (delta == 0) {
         m = 0;
         c = 0;
         error = 0;
         return;
      }
      m = (n * sum_xy - sum_x * sum_y) / delta;
      c = (sum_y - m * sum_x) / static_cast<long double>(n);
      // DEBUG_BLOCK()
      // {
      //    std::cout << "sum_x: " << sum_x << " sum_y: " << sum_y << " sum_xy: " << sum_xy << " sum_xx: " << sum_xx << " delta: " << delta <<
      //    std::endl; std::cout << "m: " << m << " c: " << c << std::endl;
      // }
      // compute the mean squared error
      error = 0;
      for (auto i = 0ul; i < n; ++i) {
         auto idx = m * keys[i] + c;
         auto got_index = predict(keys[i]);
         // ensure((idx - 0.5) <= got_index && got_index <= (idx + 0.5));
         auto cal_error = static_cast<ptrdiff_t>(i) - static_cast<ptrdiff_t>(got_index);
         size_t abs_error = (cal_error >= 0) ? cal_error : -cal_error;
         // ensure(abs_error >= 0);
         // std::cout << " key: " << keys[i] << " expected_index: " << i << " predicted_index: " << got_index << " cal_error: " << abs_error `<<
         // std::endl;
         if (abs_error > error) {
            error = abs_error;
            // std::cout << "error: " << error << std::endl;
         }
      }
      // DEBUG_BLOCK()
      // {
      //    std::cout << "error: " << error << std::endl;
      // }
   }

   int binary_search(const std::vector<keytype>& keys, const keytype key, const size_t lower, const size_t end) const
   {
      size_t l = lower, r = end;
      while (l < r) {
         int m = (l + r) / 2;
         if (keys[m] == key) {
            return m;
         } else if (keys[m] < key) {
            l = m + 1;
         } else {
            r = m;
         }
      }
      return l;
   }

   int exponential_search(const std::vector<keytype>& keys, const keytype key, size_t pos) const
   {
      // if (keys.size() == 0) {
      //    return 0;
      // }
      pos = pos >= keys.size() ? keys.size() - 1 : pos;

      auto begin = 0, end = keys.size();
      auto step = 1;
      if (keys[pos] == key) {
         return pos;
      } else if (keys[pos] < key) {
         begin = pos;
         end = pos + step;
         while (end < keys.size()) {
            if (keys[end] == key) {
               return end;
            } else if (keys[end] > key) {
               break;
            }
            step *= 2;
            begin = end;
            end = begin + step;
         }
         if (end >= keys.size()) {
            end = keys.size();
         }
      } else {
         end = pos;
         begin = pos - step;
         while (begin >= 0) {
            if (keys[begin] == key) {
               return begin;
            } else if (keys[begin] < key) {
               break;
            }
            step *= 2;
            end = begin;
            begin = end - step;
         }
         if (begin < 0) {
            begin = 0;
         }
      }

      while (end > begin) {
         size_t mid = (begin + end) / 2;
         if (keys[mid] == key) {
            return mid;
         } else if (keys[mid] < key) {
            begin = mid + 1;
         } else {
            end = mid;
         }
      }
      return begin;
   }
   // search for a key in the index and return its index
   inline size_t predict(const keytype key) const
   {
      // return std::fma(m, key, c);
      auto predict = m * key + c;
      // return predict;
      return ceil(predict);
      // return round(predict);
      // return floor(predict);
      // return predict;
   }
   inline SearchBound get_searchbound(size_t estimate, size_t array_size) const
   {
      const size_t begin = (estimate < error) ? 0 : (estimate - error);
      const size_t end = ((estimate + error + 1) > array_size) ? array_size : (estimate + error + 1);
      return SearchBound{begin, end};
   }
   u64 version = std::numeric_limits<u64>::max();
   long double m, c;
   // TODO:: remove if we are using exponential search
   size_t error = 0;
   // get the error of the trained index
   inline size_t get_error() const { return error; }
};

template <typename KEY, typename PID>
void store_models_to_file(const std::string& filename, const ska::flat_hash_map<PID, learnedindex<KEY>>& models)
{
   std::ofstream file(filename, std::ios::binary);
   if (!file.is_open()) {
      throw std::runtime_error("Failed to open file for writing");
   }
   for (const auto& [pid, model] : models) {
      file.write(reinterpret_cast<const char*>(&pid), sizeof(pid));
      file.write(reinterpret_cast<const char*>(&model), sizeof(model));
   }
   file.close();
}

template <typename KEY, typename PID>
ska::flat_hash_map<PID, learnedindex<KEY>> load_models_from_file(const std::string& filename)
{
   std::ifstream file(filename, std::ios::binary);
   if (!file.is_open()) {
      throw std::runtime_error("Failed to open file for reading");
   }
   ska::flat_hash_map<PID, learnedindex<KEY>> models;
   while (file.peek() != EOF) {
      PID pid;
      learnedindex<KEY> model;
      file.read(reinterpret_cast<char*>(&pid), sizeof(pid));
      file.read(reinterpret_cast<char*>(&model), sizeof(model));
      models[pid] = model;
   }
   file.close();
   return models;
}