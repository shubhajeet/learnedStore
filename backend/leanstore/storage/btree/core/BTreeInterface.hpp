#pragma once
#include "BTreeNode.hpp"
#include "leanstore/compileConst.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
enum class WAL_LOG_TYPE : u8 {
   WALInsert = 1,
   WALUpdate = 2,
   WALRemove = 3,
   WALAfterBeforeImage = 4,
   WALAfterImage = 5,
   WALLogicalSplit = 10,
   WALInitPage = 11
};
struct WALEntry {
   WAL_LOG_TYPE type;
};
// -------------------------------------------------------------------------------------
enum class OP_RESULT : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, NOT_ENOUGH_SPACE = 4, OTHER = 5 };
struct WALUpdateGenerator {
   void (*before)(u8* tuple, u8* entry);
   void (*after)(u8* tuple, u8* entry);
   u16 entry_size;
};
// -------------------------------------------------------------------------------------
// Interface
class BTreeInterface
{
  public:
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT lookup_simulate_long_tail(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT fast_tail_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT lookup_lr(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT lookup_lr(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT fast_trained_lookup_rs(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT lookup_rs(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT trained_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT fast_trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT fast_trained_lookup_new(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT fast_trained_lookup_rmi(KEY key, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) = 0;
   virtual OP_RESULT fast_insert(u8* key, u16 key_length, u8* value, u16 value_length) = 0;
   virtual OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0}) = 0;
   virtual OP_RESULT remove(u8* key, u16 key_length) = 0;
   virtual OP_RESULT scanAscAll(function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) = 0;
   virtual OP_RESULT scanAscAllSeg(function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) = 0;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             function<void()>,
                             u64 rlength = 0) = 0;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              function<void()>) = 0;
   virtual OP_RESULT scanAscSeg(KEY key,
                                u8* start_key,
                                u16 key_length,
                                function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                                function<void()>,
                                u64 rlength = 0) = 0;
   virtual OP_RESULT scanDescSeg(u8* start_key,
                                 u16 key_length,
                                 function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                                 function<void()>) = 0;
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() = 0;
   virtual u64 countEntries() = 0;
   virtual u64 getHeight() = 0;
   virtual void auto_train(const int maxerror = 0) = 0;
   virtual void train(const int maxerror) = 0;
   virtual void fast_train(const int maxerror) = 0;
   virtual void forced_train(const int maxerror) = 0;
   virtual void train_lr() = 0;
   virtual void stats() = 0;
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
using Slice = std::basic_string_view<u8>;
using StringU = std::basic_string<u8>;
struct MutableSlice {
   u8* ptr;
   u64 len;
   MutableSlice(u8* ptr, u64 len) : ptr(ptr), len(len) {}
   u64 length() { return len; }
   u8* data() { return ptr; }
};
}  // namespace leanstore
