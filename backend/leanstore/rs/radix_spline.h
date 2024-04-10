#pragma once
#include <algorithm>
#include <cassert>
#include <cmath>
#include <vector>
#include "builder.hpp"

namespace spline
{

struct SearchBound {
   size_t begin;
   size_t end;  // Exclusive.
};
// Approximates a cumulative distribution function (CDF) using spline
// interpolation.
template <class KeyType>
class RadixSpline
{
  public:
   RadixSpline() = default;

   RadixSpline(size_t max_error, size_t leaf_size, std::vector<spline::Coord<KeyType>> spline_points)
       : max_error_(max_error), leaf_size_(leaf_size), spline_points_(std::move(spline_points))
   {
   }

   inline bool is_within(const KeyType key, const int index) const
   {
      // return (index > 0 && index < spline_points_.size()) ? ((spline_points_[index - 1].x <= key) && (key <= spline_points_[index].x)) : false;
      return (index > 0 && index < spline_points_.size()) ? ((spline_points_[index - 1].x <= key) && (key < spline_points_[index].x)) : false;
   }
   // Returns the estimated position of `key`.
   inline double GetEstimatedPosition(const KeyType key, const int index) const
   {
      // Find spline segment with `key` ∈ (spline[index - 1], spline[index]].
      const Coord<KeyType> down = spline_points_[index - 1];
      const Coord<KeyType> up = spline_points_[index];
      // Compute slope.
      const double x_diff = up.x - down.x;
      const double y_diff = up.y - down.y;
      const double slope = y_diff / x_diff;

      // Interpolate.
      const double key_diff = key - down.x;
      auto pos = std::fma(key_diff, slope, down.y);
      return pos;
   }

   // Returns the estimated position of `key`.
   inline size_t GetEstimatedPosition(const KeyType key, const int index, std::vector<KeyType>& keys) const
   {
      auto pos = GetEstimatedPosition(key, index);
#ifdef RS_EXPONENTIAL_SEARCH
      return exponentialSearch(key, keys, pos);
#else
      auto bound = GetSearchBound(pos);
      return binarySearch(key, keys, bound.begin, bound.end);
#endif
   }

   static size_t exponentialSearch(const KeyType& key, std::vector<KeyType>& keys, size_t pos)
   {
      if (keys.size() == 0) {
         return 0;
      }
      auto begin = 0;
      auto end = keys.size();
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
            begin = end;
            step *= 2;
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
            end = begin;
            step *= 2;
            begin = end - step;
         }
         if (begin < 0) {
            begin = 0;
         }
      }
      return binarySearch(key, keys, begin, end);
   }
   static size_t binarySearch(const KeyType& key, std::vector<KeyType>& keys, const size_t& begin, const size_t& end)
   {
      auto lower = begin;
      auto upper = end;
      while (lower < upper) {
         auto mid = (lower + upper) / 2;
         if (keys[mid] == key) {
            return mid;
         } else if (keys[mid] < key) {
            lower = mid + 1;
         } else {
            upper = mid;
         }
      }
      return lower;
   }

   // Returns the estimated position of `key`.
   inline double GetEstimatedPosition(const KeyType key) const
   {
      // Find spline segment with `key` ∈ (spline[index - 1], spline[index]].
      const size_t index = GetSplineSegment(key);
      return (spline_points_.size() > index && index > 0) ? GetEstimatedPosition(key, index) : -1;
   }

   // Returns a search bound [begin, end) around the estimated position.
   inline SearchBound GetSearchBound(size_t estimate) const
   {
      // std::cout << "Estimate: " << estimate << " Max Error: " << max_error_ << std::endl;
      const size_t begin = (estimate < max_error_) ? 0 : (estimate - max_error_);
      const size_t end = (estimate + max_error_ + 1 > leaf_size_) ? leaf_size_ : (estimate + max_error_);
      // std::cout << "Begin: " << begin << " End: " << end << std::endl;
      return SearchBound{begin, end};
   }

   // Returns the size in bytes.
   inline size_t GetSize() const { return spline_points_.size(); }

   // Returns the index of the spline point that marks the end of the spline
   // segment that contains the `key`: `key` ∈ (spline[index - 1], spline[index]]
   inline size_t GetSplineSegment(const KeyType key) const
   {
      // const auto lb = std::lower_bound(spline_points_.begin(), spline_points_.end(), key,
      //                                  [](const Coord<KeyType>& coord, const KeyType key) { return coord.x <= key; });
      // return std::distance(spline_points_.begin(), lb);
      auto lower = 0;
      auto upper = spline_points_.size();
      while (lower < upper) {
         auto mid = (lower + upper) / 2;
         if (spline_points_[mid].x == key) {
            return mid;
         } else if (spline_points_[mid].x < key) {
            lower = mid + 1;
         } else {
            upper = mid;
         }
      }
      return lower;
   }

   inline bool WithinSpline(const KeyType key) const { return spline_points_[0].x <= key && key <= spline_points_[GetSize()].x; }

  public:
   size_t max_error_;
   size_t leaf_size_;

   std::vector<spline::Coord<KeyType>> spline_points_;

  private:
   template <typename>
   friend class Serializer;
};

}  // namespace spline