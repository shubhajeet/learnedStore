#include "binarySearchSIMD.hpp"
#ifdef __AVX512F__
#include <immintrin.h>  // Include the necessary header file for SIMD operations

int binarySearchSIMD(const std::vector<uint32_t>& arr, int predict, int maxerror, uint32_t target)
{
   auto size = arr.size();
   const auto bucketSize = 512 / sizeof(uint32_t) / 8;
   int lower = (predict - maxerror) / bucketSize;
   lower = (lower < 0 ? 0 : lower);
   int upper = (predict + maxerror) / bucketSize;
   upper = (upper > (size - 1) / bucketSize) ? (size - 1) / bucketSize : upper;
   // Cast the target to __m512i (SIMD data type for 64-bit integers)
   __m512i simdTarget = _mm512_set1_epi32(static_cast<int32_t>(target));
   while (lower < upper) {
      int mid = (lower + upper) / 2;

      // Load the elements from the array into SIMD registers
      __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[mid * bucketSize]);
      // Create a SIMD mask for elements greater than the target
      __mmask8 greaterMask = _mm512_cmpge_epu32_mask(simdArr, simdTarget);
      // Check if any element is greater than the target
      if (greaterMask == 0) {
         lower = mid + 1;
      } else if (greaterMask == 0xFF) {
         upper = mid;
      } else {
         // Find the position of the first set bit
         int index = __builtin_ctz(greaterMask);
         return mid * bucketSize + index;
      }
   }
   // Load the elements from the array into SIMD registers
   __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[lower * bucketSize]);
   // Create a SIMD mask for elements greater than the target
   __mmask8 greaterMask = _mm512_cmpge_epu32_mask(simdArr, simdTarget);
   // Element not found
   int index = __builtin_ctz(greaterMask);
   index = lower * bucketSize + index;
   return (index < size) ? index : size;
}

int binarySearchSIMD(const std::vector<uint64_t>& arr, int predict, int maxerror, uint64_t target)
{
   auto size = arr.size();
   const auto bucketSize = 512 / sizeof(uint64_t) / 8;
   int lower = (predict - maxerror) / bucketSize;
   lower = (lower < 0 ? 0 : lower);
   int upper = (predict + maxerror) / bucketSize;
   upper = (upper > (size - 1) / bucketSize) ? (size - 1) / bucketSize : upper;
   // Cast the target to __m512i (SIMD data type for 64-bit integers)
   __m512i simdTarget = _mm512_set1_epi64(static_cast<int64_t>(target));
   while (lower < upper) {
      int mid = (lower + upper) / 2;

      // Load the elements from the array into SIMD registers
      __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[mid * bucketSize]);
      // Create a SIMD mask for elements greater than the target
      __mmask8 greaterMask = _mm512_cmpge_epu64_mask(simdArr, simdTarget);
      // Check if any element is greater than the target
      if (greaterMask == 0) {
         lower = mid + 1;
      } else if (greaterMask == 0xFF) {
         upper = mid;
      } else {
         // Find the position of the first set bit
         int index = __builtin_ctz(greaterMask);
         return mid * bucketSize + index;
      }
   }
   // Load the elements from the array into SIMD registers
   __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[lower * bucketSize]);
   // Create a SIMD mask for elements greater than the target
   __mmask8 greaterMask = _mm512_cmpge_epu64_mask(simdArr, simdTarget);
   // Element not found
   int index = __builtin_ctz(greaterMask);
   index = lower * bucketSize + index;
   return (index < size) ? index : size;
}

int binarySearchSIMD(const std::vector<int64_t>& arr, int predict, int maxerror, uint64_t target)
{
   auto size = arr.size();
   const auto bucketSize = 512 / sizeof(int64_t) / 8;
   // int lower = 0;
   // int upper = (size - 1) / bucketSize;
   int lower = (predict - maxerror) / bucketSize;
   lower = (lower < 0 ? 0 : lower);
   int upper = (predict + maxerror) / bucketSize;
   upper = (upper > (size - 1) / bucketSize ? (size - 1) / bucketSize : upper);
   // Cast the target to __m128i
   __m512i simdTarget = _mm512_set1_epi64(static_cast<int64_t>(target));
   while (lower < upper) {
      int mid = (lower + upper) / 2;

      // Load the elements from the array into SIMD registers
      __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[mid * bucketSize]);
      // Create a SIMD mask for elements greater than the target
      __mmask8 greaterMask = _mm512_cmpge_epi64_mask(simdArr, simdTarget);
      // Extract the most significant bit from the mask
      // int cmpMask = _mm_movemask_epi8(greaterMask);
      // Check if any element is greater than the target
      if (greaterMask == 0) {
         lower = mid + 1;
         ;
      } else if (greaterMask == 0b11111111) {
         upper = mid;
      } else {
         // Find the position of the first set bit
         int index = __builtin_ctz(greaterMask);
         return mid * bucketSize + index;
      }
   }
   // Load the elements from the array into SIMD registers
   __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[lower * bucketSize]);
   // Create a SIMD mask for elements greater than the target
   __mmask8 greaterMask = _mm512_cmpge_epi64_mask(simdArr, simdTarget);
   // Element not found
   int index = __builtin_ctz(greaterMask);
   index = lower * bucketSize + index;
   return (index < size) ? index : size;
}

// int binarySearchSIMD(const std::vector<uint32_t>& arr, int predict, int maxerror, uint32_t target)
// {
//    auto size = arr.size();
//    const auto bucketSize = 512 / sizeof(uint32_t);
//    int lower = 0;
//    int upper = (size - 1) / bucketSize;
//    int lower = 0;
//    int upper = (size - 1) / bucketSize;
//    // Cast the target to __m128i
//    __m512i simdTarget = _mm512_set1_epi32(static_cast<int64_t>(target));
//    while (lower <= upper) {
//       int mid = (lower + upper) / 2;

//       // Load the elements from the array into SIMD registers
//       __m512i simdArr = _mm512_loadu_si512((__m512i*)&arr[mid * bucketSize]);
//       // Create a SIMD mask for elements greater than the target
//       __mmask16 greaterMask = _mm512_cmpgt_epi32_mask(simdArr, simdTarget);
//       // Extract the most significant bit from the mask
//       // int cmpMask = _mm_movemask_epi8(greaterMask);
//       // Check if any element is greater than the target
//       if (greaterMask == 0) {
//          lower = mid + 1;
//          ;
//       } else if (greaterMask == 0b1111111111111111) {
//          upper = mid;
//       } else {
//          // Find the position of the first set bit
//          int index = __builtin_ctz(greaterMask);
//          return mid * bucketSize + index;
//       }
//    }

//    // Element not found
//    return lower * bucketSize;
// }
#endif