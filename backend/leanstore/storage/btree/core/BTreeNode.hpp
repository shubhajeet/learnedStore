#pragma once
#include <immintrin.h>
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>
#include <string>
// #define USE_TAG
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
struct BTreeNode;
using SwipType = Swip<BTreeNode>;
using HeadType = u32;
// -------------------------------------------------------------------------------------
static inline u64 swap(u64 x)
{
   return __builtin_bswap64(x);
}
static inline u32 swap(u32 x)
{
   return __builtin_bswap32(x);
}
static inline u16 swap(u16 x)
{
   return __builtin_bswap16(x);
}
// -------------------------------------------------------------------------------------
struct BTreeNodeHeader {
   static const u16 underFullSize = EFFECTIVE_PAGE_SIZE * 0.6;
   static const u16 K_WAY_MERGE_THRESHOLD = EFFECTIVE_PAGE_SIZE * 0.45;

   struct SeparatorInfo {
      u16 length;
      u16 slot;
      bool trunc;  // TODO: ???
   };

   struct FenceKey {
      u16 offset;
      u16 length;
   };

   Swip<BTreeNode> upper = nullptr;
   FenceKey lower_fence = {0, 0};
   FenceKey upper_fence = {0, 0};

   u16 count = 0;  // count number of separators, excluding the upper swip
   bool is_leaf;
   u16 space_used = 0;  // does not include the header, but includes fences !!!!!
   u16 data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
   u16 prefix_length = 0;

   static const u16 hint_count = 16;
   union {
      u32 hint[hint_count];
      u8 tag[hint_count * 4];
   };
   // u32 hint[hint_count];

   BTreeNodeHeader(bool is_leaf) : is_leaf(is_leaf) {}
   ~BTreeNodeHeader() {}

   inline u8* ptr() { return reinterpret_cast<u8*>(this); }
   inline bool isInner() { return !is_leaf; }
   inline u8* getLowerFenceKey() { return lower_fence.offset ? ptr() + lower_fence.offset : nullptr; }
   inline u8* getUpperFenceKey() { return upper_fence.offset ? ptr() + upper_fence.offset : nullptr; }
   inline bool isUpperFenceInfinity() { return !upper_fence.offset; };
   inline bool isLowerFenceInfinity() { return !lower_fence.offset; };
};
// -------------------------------------------------------------------------------------
struct BTreeNode : public BTreeNodeHeader {
   struct __attribute__((packed)) Slot {
      // Layout:  key wihtout prefix | Payload
      u16 offset;
      u16 key_len;
      u16 payload_len : 15;
      // MyNote: add pointer to segment
      // u16 seg_ptr = 0;
      union {
         HeadType head;
         u8 head_bytes[4];
      };
   };
   static constexpr u64 btree_node_header_size = sizeof(BTreeNodeHeader);
   static constexpr u64 slot_size = sizeof(Slot);
   static constexpr u64 pure_slots_capacity = (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) / (sizeof(Slot));
   static constexpr u64 left_space_to_waste = (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) % (sizeof(Slot));
   Slot slot[pure_slots_capacity];
   u8 padding[left_space_to_waste];

   BTreeNode(bool is_leaf) : BTreeNodeHeader(is_leaf) {}

   u16 freeSpace() { return data_offset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
   u16 freeSpaceAfterCompaction() { return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used; }
   // -------------------------------------------------------------------------------------
   double fillFactorAfterCompaction() { return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE)); }
   // -------------------------------------------------------------------------------------
   bool hasEnoughSpaceFor(u32 space_needed) { return (space_needed <= freeSpace() || space_needed <= freeSpaceAfterCompaction()); }
   // ATTENTION: this method has side effects !
   bool requestSpaceFor(u16 space_needed)
   {
      if (space_needed <= freeSpace())
         return true;
      if (space_needed <= freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }
   // -------------------------------------------------------------------------------------
   inline u8* getKey(u16 slotId) { return ptr() + slot[slotId].offset; }
   inline u16 getKeyLen(u16 slotId) { return slot[slotId].key_len; }
   inline u16 getFullKeyLen(u16 slotId) { return prefix_length + getKeyLen(slotId); }
   inline u16 getPayloadLength(u16 slotId) { return slot[slotId].payload_len; }
   inline void shortenPayload(u16 slotId, u16 len)
   {
      assert(len < slot[slotId].payload_len);
      const u16 freed_space = slot[slotId].payload_len - len;
      space_used -= freed_space;
      slot[slotId].payload_len = len;
   }
   inline u8* getPayload(u16 slotId) { return ptr() + slot[slotId].offset + slot[slotId].key_len; }
   inline SwipType& getChild(u16 slotId) { return *reinterpret_cast<SwipType*>(getPayload(slotId)); }
   // -------------------------------------------------------------------------------------
   inline u8* getPrefix() { return getLowerFenceKey(); }
   inline void copyPrefix(u8* out) { memcpy(out, getLowerFenceKey(), prefix_length); }
   inline void copyKeyWithoutPrefix(u16 slotId, u8* out_after_prefix) { memcpy(out_after_prefix, getKey(slotId), getKeyLen(slotId)); }
   inline void copyFullKey(u16 slotId, u8* out)
   {
      memcpy(out, getPrefix(), prefix_length);
      memcpy(out + prefix_length, getKey(slotId), getKeyLen(slotId));
   }
   // -------------------------------------------------------------------------------------
   static inline s32 cmpKeys(const u8* a, const u8* b, u16 aLength, u16 bLength)
   {
      u16 length = min(aLength, bLength);
      if (length < 4) {
         while (length-- > 0) {
            if (*a++ != *b++)
               return a[-1] < b[-1] ? -1 : 1;
         }
         return (aLength - bLength);
      } else {
         int c = memcmp(a, b, length);
         if (c != 0)
            return c;
         return (aLength - bLength);
      }
   }
   static inline HeadType head(const u8* key, u16& keyLength)
   {
      switch (keyLength) {
         case 0:
            return 0;
         case 1:
            return static_cast<u32>(key[0]) << 24;
         case 2:
            return static_cast<u32>(__builtin_bswap16(*reinterpret_cast<const u16*>(key))) << 16;
         case 3:
            return (static_cast<u32>(__builtin_bswap16(*reinterpret_cast<const u16*>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
         default:
            return __builtin_bswap32(*reinterpret_cast<const u32*>(key));
      }
   }
   void makeHint();
   // -------------------------------------------------------------------------------------
   s32 compareKeyWithBoundaries(const u8* key, u16 keyLength);
   // -------------------------------------------------------------------------------------
   void searchHint(u32 keyHead, unsigned& pos, unsigned& pos2)
   {
      for (pos = 0; pos < hint_count; pos++)
         if (hint[pos] >= keyHead)
            break;
      for (pos2 = pos; pos2 < hint_count; pos2++)
         if (hint[pos2] != keyHead)
            break;
   }
   template <bool equalityOnly = false>
   int searchTagEq(const u8* key, u16 keyLength)
   {
      auto hash = getStringHash(key, keyLength);
      // Now 'vec' contains the 16 integers from 'b'
      __m512i vec = _mm512_loadu_si512(tag);
      // a is now filled with the 32-bit integer value 5 in all 16 of its slots
      __m512i mapping1 = _mm512_set1_epi8(hash);
      __mmask64 result1 = _mm512_cmpeq_epi8_mask(mapping1, vec);
      int position = 64 - _tzcnt_u64(result1);
      auto keyHead = head(key, keyLength);
      // Print the positions of the set (1) bits
      while (result1 != 0) {
         position = 64 - _tzcnt_u64(result1);

         if (slot[position].key_len <= 4) {
            // head is equal, we don't have to check the rest of the key
            if (position < count && keyHead == slot[position].head) {
               return position;
            }
         } else {
            int cmp = cmpKeys(key, getKey(position), keyLength, getKeyLen(position));
            if (cmp == 0 && position < count && keyHead == slot[position].head) {
               return position;  // It is even equal
            }
         }
         result1 &= (result1 - 1);  // Clear the lowest set bit
      }
      return -1;
   }
   void searchHintEq(u32 keyHead, unsigned& pos, unsigned& pos2)
   {
      // std::cout << "using simd" << std::endl;
      __m512i vec = _mm512_loadu_si512(hint);
      __m512i mapping1 = _mm512_set1_epi32(keyHead);
      __mmask16 result1 = _mm512_cmpeq_epi32_mask(mapping1, vec);
      pos = __builtin_ctz(result1);
      pos2 = pos + __builtin_popcount(result1);
   }

   void searchHintGe(u32 keyHead, unsigned& pos, unsigned& pos2)
   {
      __m512i vec = _mm512_loadu_si512(hint);
      __m512i mapping1 = _mm512_set1_epi32(keyHead);
      __mmask16 result1 = _mm512_cmpge_epu32_mask(mapping1, vec);
      __mmask16 result2 = _mm512_cmple_epu32_mask(mapping1, vec);
      pos = __builtin_ctz(result1);
      // __m512i nvec = _mm512_loadu_si512(hint[pos]);
      // __mmask16 result2 = _mm512_cmpeq_epu32_mask(mapping1, nvec);
      pos2 = 32 - __builtin_ctz(result2);
   }
   // -------------------------------------------------------------------------------------
   template <bool equalityOnly = false>
   s16 exponentialSearch(const u8* key, u16 keyLength, s16 pos, bool* isequal = nullptr)
   {
      // if (count == 0)
      //    return 0;
      pos = (pos >= count) ? count - 1 : pos;
      key += prefix_length;
      keyLength -= prefix_length;
      HeadType keyHead = head(key, keyLength);

      int begin_i = 0, end_i = count - 1;
      size_t step = 1;
      auto cmp = [&](int i) {
         if (keyHead < slot[i].head) {
            return -1;
         } else if (keyHead > slot[i].head) {
            return 1;
         } else if (slot[i].key_len <= 4) {
            if (keyLength < slot[i].key_len) {
               return -1;
            } else if (keyLength > slot[i].key_len) {
               return 1;
            } else {
               return 0;
            }
         } else {
            return cmpKeys(key, getKey(i), keyLength, getKeyLen(i));
            // auto cmp = cmpKeys(key, getKey(i), keyLength, getKeyLen(i));
            // if (cmp < 0) {
            //    return -1;
            // } else if (cmp > 0) {
            //    return 1;
            // } else {
            //    return 0;
            // }
         }
      };
      auto cmppos = cmp(pos);
      if (cmppos == 0) {
         if (isequal != nullptr) {
            *isequal = true;
         }
         return pos;
      } else if (cmppos > 0) {
         begin_i = pos;
         end_i = begin_i + step;
         while (end_i < (int)count) {
            cmppos = cmp(end_i);
            if (cmppos == 0) {
               if (isequal != nullptr) {
                  *isequal = true;
               }
               return end_i;
            } else if (cmppos < 0) {
               break;
            }
            step *= 2;
            begin_i = end_i;
            end_i = begin_i + step;
         }
         if (end_i >= (int)count) {
            end_i = count - 1;
         }
      } else {
         end_i = pos;
         begin_i = end_i - step;
         while (begin_i >= 0) {
            cmppos = cmp(begin_i);
            if (cmppos == 0) {
               if (isequal != nullptr) {
                  *isequal = true;
               }
               return begin_i;
            } else if (cmppos > 0) {
               break;
            }
            step *= 2;
            end_i = begin_i;
            begin_i = end_i - step;
         }
         if (begin_i < 0) {
            begin_i = 0;
         }
      }

      // assert(begin_i >= 0);
      // assert(end_i < (int)count);
      // assert(begin_i <= end_i);
      auto lower = begin_i;
      auto upper = end_i;
      while (lower < upper) {
         u16 mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else if (slot[mid].key_len <= 4) {
            // head is equal, we don't have to check the rest of the key
            if (keyLength < slot[mid].key_len) {
               upper = mid;
            } else if (keyLength > slot[mid].key_len) {
               lower = mid + 1;
            } else {
               if (isequal != nullptr) {
                  *isequal = true;
               }
               return mid;  // It is even equal
            }
         } else {
            int cmp = cmpKeys(key, getKey(mid), keyLength, getKeyLen(mid));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (isequal != nullptr) {
                  *isequal = true;
               }
               return mid;  // It is even equal
            }
         }
      }
      if constexpr (equalityOnly) {
         if (cmp(lower) == 0) {
            if (isequal != nullptr) {
               *isequal = true;
            }
            return lower;
         }
         return -1;
      }
      return lower;
   }
   template <bool equalityOnly = false>
   s16 binarySearch(const u8* key, u16 keyLength, s16 min_pos, s16 max_pos, bool* isequal = nullptr)
   {
      // the compared key has the same prefix
      key += prefix_length;
      keyLength -= prefix_length;

      auto lower = min_pos;
      auto upper = max_pos;
      HeadType keyHead = head(key, keyLength);
      while (lower < upper) {
         u16 mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else if (slot[mid].key_len <= 4) {
            // head is equal, we don't have to check the rest of the key
            if (keyLength < slot[mid].key_len) {
               upper = mid;
            } else if (keyLength > slot[mid].key_len) {
               lower = mid + 1;
            } else {
               *isequal = true;
               return mid;  // It is even equal
            }
         } else {
            int cmp = cmpKeys(key, getKey(mid), keyLength, getKeyLen(mid));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               *isequal = true;
               return mid;  // It is even equal
            }
         }
      }
      if constexpr (equalityOnly) {
         auto cmp = [&](int i) {
            if (keyHead < slot[i].head) {
               return -1;
            } else if (keyHead > slot[i].head) {
               return 1;
            } else if (slot[i].key_len <= 4) {
               if (keyLength < slot[i].key_len) {
                  return -1;
               } else if (keyLength > slot[i].key_len) {
                  return 1;
               } else {
                  return 0;
               }
            } else {
               return cmpKeys(key, getKey(i), keyLength, getKeyLen(i));
            }
         };

         if (cmp(lower) == 0) {
            if (isequal != nullptr) {
               *isequal = true;
            }
            return lower;
         }
         return -1;
      }
      return lower;
   }
   // Returns the position where the key[pos] (if exists) >= key (not less than the given key)
   // Asc: (2) (2) (1) -> (2) (2) (1) (0) -> (2) (2) (1) (0) (0) -> ...  -> (2) (2) (2)
   template <bool equalityOnly = false>
   s16 lowerBound(const u8* key, u16 keyLength, bool* is_equal = nullptr)
   {
      if (is_equal != nullptr && is_leaf) {
         *is_equal = false;
      }
      if constexpr (equalityOnly) {
         if ((keyLength < prefix_length) || (bcmp(key, getLowerFenceKey(), prefix_length) != 0))
            return -1;
      } else {
         int prefixCmp = cmpKeys(key, getLowerFenceKey(), min<u16>(keyLength, prefix_length), prefix_length);
         if (prefixCmp < 0)
            return 0;
         else if (prefixCmp > 0)
            return count;
      }
      // the compared key has the same prefix
      key += prefix_length;
      keyLength -= prefix_length;

      u16 lower = 0;
      u16 upper = count;
      HeadType keyHead = head(key, keyLength);

#ifndef SIMD_SEARCH_HINT
      if (count > hint_count * 2) {
         unsigned dist = count / (hint_count + 1);
         unsigned pos, pos2;
         // MyNote: What is searchHint
         searchHint(keyHead, pos, pos2);
         lower = pos * dist;
         if (pos2 < hint_count)
            upper = (pos2 + 1) * dist;
      }
#else
      unsigned dist = count / (hint_count + 1);
#ifdef USE_TAG
      if (is_leaf && count < 64) {
         auto slotId = searchTagEq(key, keyLength);
         if (slotId != -1) {
            if (is_equal != nullptr) {
               *is_equal = true;
            }
            return slotId;
         }
      }
#else
      if (dist < 1 && count > 1 && is_leaf) {
         unsigned pos, pos2;
         searchHintEq(keyHead, pos, pos2);
         // searchHintGe(keyHead, pos, pos2);
         lower = (pos < count) ? pos : 0;
         upper = (pos2 < count) ? pos2 + 1 : count;
      }
#endif
      if (count > hint_count * 2 && !is_leaf) {
         unsigned pos, pos2;
         // MyNote: What is searchHint
         // searchHintGe(keyHead, pos, pos2);
         searchHint(keyHead, pos, pos2);
         lower = pos * dist;
         if (pos2 < hint_count)
            upper = (pos2 + 1) * dist;
      }
#endif
      while (lower < upper) {
         u16 mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else if (slot[mid].key_len <= 4) {
            // head is equal, we don't have to check the rest of the key
            if (keyLength < slot[mid].key_len) {
               upper = mid;
            } else if (keyLength > slot[mid].key_len) {
               lower = mid + 1;
            } else {
               if (is_equal != nullptr && is_leaf) {
                  *is_equal = true;
               }
               return mid;  // It is even equal
            }
         } else {
            int cmp = cmpKeys(key, getKey(mid), keyLength, getKeyLen(mid));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (is_equal != nullptr && is_leaf) {
                  *is_equal = true;
               }
               return mid;  // It is even equal
            }
         }
      }
      if constexpr (equalityOnly)
         return -1;
      return lower;
   }
   // -------------------------------------------------------------------------------------
   void updateHint(u16 slotId);
   // -------------------------------------------------------------------------------------
   s16 insertDoNotCopyPayload(const u8* key, u16 key_len, u16 payload_len);
   s32 insert(const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   static u16 spaceNeeded(u16 keyLength, u16 payload_len, u16 prefixLength);
   u16 spaceNeeded(u16 key_length, u16 payload_len);
   bool canInsert(u16 key_length, u16 payload_len);
   bool prepareInsert(u16 keyLength, u16 payload_len);
   // -------------------------------------------------------------------------------------
   bool update(u8* key, u16 keyLength, u16 payload_length, u8* payload);
   // -------------------------------------------------------------------------------------
   void compactify();
   // -------------------------------------------------------------------------------------
   // merge right node into this node
   u32 mergeSpaceUpperBound(ExclusivePageGuard<BTreeNode>& right);
   u32 spaceUsedBySlot(u16 slot_id);
   // -------------------------------------------------------------------------------------
   bool merge(u16 slotId, ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& right);
   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, u16 count);
   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot);
   void insertFence(FenceKey& fk, u8* key, u16 keyLength);
   void setFences(u8* lowerKey, u16 lowerLen, u8* upperKey, u16 upperLen);
   void split(ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& new_node, u16 sepSlot, u8* sepKey, u16 sepLength);
   u16 commonPrefix(u16 aPos, u16 bPos);
   SeparatorInfo findSep();
   void getSep(u8* sepKeyOut, SeparatorInfo info);
   Swip<BTreeNode>& lookupInner(const u8* key, u16 keyLength);
   // -------------------------------------------------------------------------------------
   // Not synchronized or todo section
   bool removeSlot(u16 slotId);
   bool remove(const u8* key, const u16 keyLength);
   u8 getStringHash(const u8* key_bytes, u16 key_length);
   u8 getKeyHash(u16 slotId);
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE, "BTreeNode must be equal to one page");
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
