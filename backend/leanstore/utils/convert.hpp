#pragma once
#include "leanstore/Config.hpp"
namespace leanstore
{
namespace utils
{
template <typename to_type>
to_type u8_to(const u8* data, const size_t length)
{
   to_type result = 0;
   auto key_len = sizeof(to_type);
   // assert(key_len >= length);
   for (auto i = 0ul; i < length; i++) {
      result <<= 8;                                 // shift left by 8 bits
      result |= static_cast<uint8_t>(*(data + i));  // add next byte to the result
   }
   for (auto i = length; i < key_len; i++) {
      result <<= 8;  // shift left by 8 bits
      const uint8_t zero = 0;
      result |= static_cast<uint8_t>(zero);
      // result |= static_cast<uint8_t>(0);
   }
   return result;
}
template <typename to_type>
to_type string_view_to(const std::basic_string_view<unsigned char> view)
{
   to_type result = 0;
   for (char c : view) {
      result <<= 8;                       // shift left by 8 bits
      result |= static_cast<uint8_t>(c);  // add next byte to the result
   }
   return result;
}
};  // namespace utils
};  // namespace leanstore