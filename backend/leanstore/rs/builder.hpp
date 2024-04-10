#pragma once

#include <cassert>
#include <cmath>
#include <limits>

namespace spline
{
template <class KType>
struct Coord {
   KType x;
   double y;
};

// Allows building a `RadixSpline` in a single pass over sorted data.
template <class KeyType>
class Builder
{
  public:
   Builder(size_t max_error = 32, size_t start_position = 0)
       : max_error_(max_error), curr_num_keys_(0), curr_num_distinct_keys_(0), prev_key_(0), prev_position_(0), start_pos_(start_position)
   {
   }

   // Adds a key. Assumes that keys are stored in a dense array.
   void AddKey(KeyType key)
   {
      if (curr_num_keys_ == 0) {
         // AddKey(key, /*position=*/0);
         AddKey(key, start_pos_);
         return;
      }
      AddKey(key, prev_position_ + 1);
   }

   std::vector<Coord<KeyType>> Finalize()
   {
      // Last key needs to be equal to `max_key_`.

      // Ensure that `prev_key_` (== `max_key_`) is last key on spline.
      if (curr_num_keys_ > 0 && spline_points_.back().x != prev_key_) {
         // AddKeyToSpline(prev_key_, prev_ptr_, prev_position_);
         AddKeyToSpline(prev_point_.x, prev_point_.y);
      }
      return std::move(spline_points_);
   }

  private:
   void AddKey(KeyType key, size_t position)
   {
      // Keys need to be monotonically increasing.
      assert(key >= prev_key_);
      // Positions need to be strictly monotonically increasing.
      assert(position == start_pos_ || position > prev_position_);

      PossiblyAddKeyToSpline(key, position);

      ++curr_num_keys_;
      prev_key_ = key;
      prev_position_ = position;
   }

   void AddKeyToSpline(KeyType key, double position) { spline_points_.push_back({key, position}); }

   enum Orientation { Collinear, CW, CCW };
   static constexpr double precision = std::numeric_limits<double>::epsilon();

   static Orientation ComputeOrientation(const double dx1, const double dy1, const double dx2, const double dy2)
   {
      const double expr = std::fma(dy1, dx2, -std::fma(dy2, dx1, 0));
      if (expr > precision)
         return Orientation::CW;
      else if (expr < -precision)
         return Orientation::CCW;
      return Orientation::Collinear;
   };

   void SetUpperLimit(KeyType key, double position) { upper_limit_ = {key, position}; }
   void SetLowerLimit(KeyType key, double position) { lower_limit_ = {key, position}; }
   void RememberPreviousCDFPoint(KeyType key, double position) { prev_point_ = {key, position}; }

   // Implementation is based on `GreedySplineCorridor` from:
   // T. Neumann and S. Michel. Smooth interpolating histograms with error
   // guarantees. [BNCOD'08]
   void PossiblyAddKeyToSpline(KeyType key, double position)
   {
      if (curr_num_keys_ == 0) {
         // Add first CDF point to spline.
         AddKeyToSpline(key, position);
         ++curr_num_distinct_keys_;
         RememberPreviousCDFPoint(key, position);
         return;
      }

      if (key == prev_key_) {
         // No new CDF point if the key didn't change.
         return;
      }

      // New CDF point.
      ++curr_num_distinct_keys_;

      if (curr_num_distinct_keys_ == 2) {
         // Initialize `upper_limit_` and `lower_limit_` using the second CDF
         // point.
         SetUpperLimit(key, position + max_error_);
         SetLowerLimit(key, (position < max_error_) ? 0 : position - max_error_);
         RememberPreviousCDFPoint(key, position);
         return;
      }

      // `B` in algorithm.
      const Coord<KeyType>& last = spline_points_.back();

      // Compute current `upper_y` and `lower_y`.
      const double upper_y = position + max_error_;
      const double lower_y = (position < max_error_) ? 0 : position - max_error_;

      // Compute differences.
      assert(upper_limit_.x >= last.x);
      assert(lower_limit_.x >= last.x);
      assert(key >= last.x);
      const double upper_limit_x_diff = upper_limit_.x - last.x;
      const double lower_limit_x_diff = lower_limit_.x - last.x;
      const double x_diff = key - last.x;

      assert(upper_limit_.y >= last.y);
      assert(position >= last.y);
      const double upper_limit_y_diff = upper_limit_.y - last.y;
      const double lower_limit_y_diff = lower_limit_.y - last.y;
      const double y_diff = position - last.y;

      // `prev_point_` is the previous point on the CDF and the next candidate to
      // be added to the spline. Hence, it should be different from the `last`
      // point on the spline.
      assert(prev_point_.x != last.x);

      // Do we cut the error corridor?
      if ((ComputeOrientation(upper_limit_x_diff, upper_limit_y_diff, x_diff, y_diff) != Orientation::CW) ||
          (ComputeOrientation(lower_limit_x_diff, lower_limit_y_diff, x_diff, y_diff) != Orientation::CCW)) {
         // Add previous CDF point to spline.
         AddKeyToSpline(prev_point_.x, prev_point_.y);

         // Update limits.
         SetUpperLimit(key, upper_y);
         SetLowerLimit(key, lower_y);
      } else {
         assert(upper_y >= last.y);
         const double upper_y_diff = upper_y - last.y;
         if (ComputeOrientation(upper_limit_x_diff, upper_limit_y_diff, x_diff, upper_y_diff) == Orientation::CW) {
            SetUpperLimit(key, upper_y);
         }

         const double lower_y_diff = lower_y - last.y;
         if (ComputeOrientation(lower_limit_x_diff, lower_limit_y_diff, x_diff, lower_y_diff) == Orientation::CCW) {
            SetLowerLimit(key, lower_y);
         }
      }

      RememberPreviousCDFPoint(key, position);
   }

   const size_t max_error_;

   std::vector<Coord<KeyType>> spline_points_;
   size_t curr_num_keys_;
   size_t curr_num_distinct_keys_;
   KeyType prev_key_;
   size_t prev_position_;
   size_t start_pos_;

   // Current upper and lower limits on the error corridor of the spline.
   Coord<KeyType> upper_limit_;
   Coord<KeyType> lower_limit_;

   // Previous CDF point.
   Coord<KeyType> prev_point_;
};

}  // namespace spline