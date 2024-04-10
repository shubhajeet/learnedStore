#pragma once
#include <cstdint>
#include <vector>
#ifdef __AVX512F__
#include <immintrin.h>

int binarySearchSIMD(const std::vector<uint64_t>& arr, int predict, int maxerror, uint64_t target);
int binarySearchSIMD(const std::vector<uint32_t>& arr, int predict, int maxerror, uint32_t target);
int binarySearchSIMD(const std::vector<int64_t>& arr, int predict, int maxerror, int32_t target);
#endif

template <typename T>
int binarySearch(const std::vector<T>& arr, int predict, int maxerror, const T& target)
{
   auto size = arr.size();
   int lower = (predict - maxerror);
   lower = (lower < 0 ? 0 : lower);
   int upper = (predict + maxerror + 1);
   upper = (upper > (size - 1)) ? size : upper;
   while (lower < upper) {
      // int mid = lower + (upper - lower) / 2;
      int mid = (upper + lower) / 2;
      if (arr[mid] == target)
         return mid;
      if (arr[mid] < target)
         lower = mid + 1;
      else
         upper = mid;
   }
   return lower;  // Element not found
}