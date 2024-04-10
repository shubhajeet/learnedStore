#pragma once
#include "Units.hpp"
#include "leanstore/fold.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/utils/convert.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
/**
unsigned fold(uint8_t* writer, const s32& x)
{
*reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
return sizeof(x);
}

unsigned fold(uint8_t* writer, const s64& x)
{
*reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
return sizeof(x);
}

unsigned fold(uint8_t* writer, const u64& x)
{
*reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
return sizeof(x);
}

unsigned fold(uint8_t* writer, const u32& x)
{
*reinterpret_cast<u32*>(writer) = __builtin_bswap32(x);
return sizeof(x);
}
**/
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeInterface {
   virtual bool lookup(Key k, Payload& v) = 0;
   virtual bool lookup_simulate_long_tail(Key k, Payload& v) = 0;
   virtual bool fast_tail_lookup(Key k, Payload& v) = 0;
   virtual bool lookup_lr(Key k, Payload& v) = 0;
   virtual bool lookup_rs(Key k, Payload& v) = 0;
   virtual bool lookup_rmi(Key k, Payload& v) = 0;
   virtual bool trained_lookup(Key k, Payload& v) = 0;
   virtual void update(Key k, Payload& v) = 0;
   virtual void insert(Key k, Payload& v) = 0;
   virtual void fast_insert(Key k, Payload& v) = 0;
   virtual void train(const int maxerror) = 0;
   virtual void fast_train(const int maxerror) = 0;
   virtual void train_lr() = 0;
   virtual void stats() = 0;
   virtual bool scan_asc_all() = 0;
   virtual bool scan_asc_all_seg() = 0;
   virtual bool scan_asc(Key k, u64& rlength) = 0;
   virtual bool scan_desc(Key k, Payload& v) = 0;
   virtual bool scan_asc_seg(Key k, u64& rlength) = 0;
   virtual bool scan_desc_seg(Key k, Payload& v) = 0;
};
// -------------------------------------------------------------------------------------
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
template <typename Key, typename Payload>
struct BTreeVSAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;

   BTreeVSAdapter(leanstore::storage::btree::BTreeInterface& btree) : btree(btree) {}
   bool lookup(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      // return btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
      //        OP_RESULT::OK;
      return btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
   }
   bool lookup_simulate_long_tail(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.lookup_simulate_long_tail(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.lookup_simulate_long_tail(key_bytes, fold(key_bytes, k),
      // [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
   }
   bool fast_tail_lookup(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.fast_tail_lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.fast_tail_lookup(key_bytes, fold(key_bytes, k),
      //                               [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
   }
   bool lookup_lr(Key k, Payload& v) override
   {
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.lookup_lr(k, [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.lookup_lr(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // u8 key_bytes[sizeof(Key)];
      // return btree.lookup_lr(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
      //        OP_RESULT::OK;
   }
   bool lookup_rs(Key k, Payload& v) override
   {
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.fast_trained_lookup_rs(k, [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.fast_trained_lookup_rs(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // return btree.lookup_rs(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // u8 key_bytes[sizeof(Key)];
      // return btree.lookup_lr(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
      //        OP_RESULT::OK;
   }
   bool lookup_rmi(Key k, Payload& v) override
   {
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.fast_trained_lookup_rmi(k, [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.fast_trained_lookup_rs(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // return btree.lookup_rs(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // u8 key_bytes[sizeof(Key)];
      // return btree.lookup_lr(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
      //        OP_RESULT::OK;
   }
   bool trained_lookup(Key k, Payload& v) override
   {
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.fast_trained_lookup_new(k, [&](const u8* payload, u16 payload_length) {
         value_ptr = payload;
         value_length = payload_length;
         return payload_length;
      }) == OP_RESULT::OK;
      // return btree.trained_lookup(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // return btree.fast_trained_lookup(k, [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) == OP_RESULT::OK;
      // u8 key_bytes[sizeof(Key)];
      // return btree.fast_lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); })
      // ==
      //        OP_RESULT::OK;
   }
   void insert(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
   }
   void fast_insert(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      btree.fast_insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
   }
   void update(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
   }

   // void fast_train(const int maxerror) override { btree.fast_train(maxerror); }
   void fast_train(const int maxerror) override { btree.forced_train(maxerror); }
   void train(const int maxerror) override { btree.train(maxerror); }
   void train_lr() override { btree.train_lr(); }

   void stats() override { btree.stats(); }

   bool scan_asc_all() override
   {
      // u8 key_bytes[sizeof(Key)];
      // std::array<u8, sizeof(Key)> key_bytes;
      // size_t value_length = 0;
      vector<KEY> output_keys;
      // const u8* value_ptr = nullptr;
      auto ret = btree.scanAscAll([&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
         output_keys.push_back(utils::u8_to<KEY>(key, key_length));
         return true;
      }) == OP_RESULT::OK;
      // std::cout << "Size of vector scanall: " << output_keys.size() << std::endl;
      // for (auto& k : output_keys)
      //    std::cout << " scanallkey: " << k << std::endl;
      return ret;
   }
   bool scan_asc_all_seg() override
   {
      // u8 key_bytes[sizeof(Key)];
      vector<KEY> output_keys;

      auto ret = btree.scanAscAllSeg([&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
         output_keys.push_back(utils::u8_to<KEY>(key, key_length));
         return true;
      }) == OP_RESULT::OK;
      // std::cout << "Size of vector: " << output_keys.size() << std::endl;
      // for (auto& k : output_keys)
      //    std::cout << "scanallsegkey: " << k << std::endl;
      return ret;
   }

   bool scan_asc(Key k, u64& rlength = 0) override
   {
      // u8 key_bytes[sizeof(Key)];
      std::array<u8, sizeof(Key)> key_bytes;
      size_t value_length = 0;
      vector<KEY> output_keys;
      if (rlength != 0) {
         output_keys.reserve(rlength);
      }
      const u8* value_ptr = nullptr;
      auto ret = btree.scanAsc(
                     key_bytes.data(), fold(key_bytes.data(), k),
                     [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
                        output_keys.push_back(utils::u8_to<KEY>(key, key_length));
                        return true;
                     },
                     []() {}, rlength) == OP_RESULT::OK;

      return ret;
   }

   bool scan_desc(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.scanDesc(
                 key_bytes, fold(key_bytes, k),
                 [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
                    if (key_length != payload_length) {
                       return false;
                    }
                    value_ptr = payload;
                    value_length = payload_length;
                    return true;
                 },
                 []() {}) == OP_RESULT::OK;
   }

   bool scan_asc_seg(Key k, u64& rlength) override
   {
      // u8 key_bytes[sizeof(Key)];
      std::array<u8, sizeof(Key)> key_bytes;
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      std::vector<KEY> output_keys;
      if (rlength != 0) {
         output_keys.reserve(rlength);
      }
      return btree.scanAscSeg(
                 k, key_bytes.data(), fold(key_bytes.data(), k),
                 [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
                    output_keys.push_back(utils::u8_to<KEY>(key, key_length));
                    return true;
                 },
                 []() {}, rlength) == OP_RESULT::OK;
   }

   bool scan_desc_seg(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      size_t value_length = 0;
      const u8* value_ptr = nullptr;
      return btree.scanDescSeg(
                 key_bytes, fold(key_bytes, k),
                 [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
                    if (key_length != payload_length) {
                       return false;
                    }
                    value_ptr = payload;
                    value_length = payload_length;
                    return true;
                 },
                 []() {}) == OP_RESULT::OK;
   }
};
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
   u8 value[size];
   BytesPayload() {}
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
   BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
   BytesPayload& operator=(const BytesPayload& other)
   {
      std::memcpy(value, other.value, sizeof(value));
      return *this;
   }
};
}  // namespace leanstore
