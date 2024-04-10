#pragma once
#include "Units.hpp"
namespace leanstore
{
unsigned fold(uint8_t* writer, const s32& x);
unsigned fold(uint8_t* writer, const s64& x);
unsigned fold(uint8_t* writer, const u64& x);
unsigned fold(uint8_t* writer, const u32& x);
}  // namespace leanstore