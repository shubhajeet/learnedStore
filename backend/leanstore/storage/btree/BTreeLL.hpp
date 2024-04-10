#pragma once
#include "core/BTreeGeneric.hpp"
#include "core/BTreeInterface.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/compileConst.hpp"
#include "leanstore/fold.hpp"
#include "leanstore/lr/learnedIndex.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/rs/builder.hpp"
#include "leanstore/rs/radix_spline.h"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "rmi.h"
#include "rs/builder.h"
#include "rs/radix_spline.h"
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
// -------------------------------------------------------------------------------------
class BTreeLL : public BTreeInterface, public BTreeGeneric
{
  public:
   struct WALBeforeAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInsert : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate : WALEntry {
      u16 key_length;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT lookup_simulate_long_tail(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT lookup_rs(KEY key, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT fast_trained_lookup_rs(const KEY key, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT fast_trained_lookup_rs(const KEY key);
   virtual OP_RESULT fast_tail_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT lookup_lr(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT lookup_lr(const KEY key, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT trained_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
   virtual OP_RESULT trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback);
   virtual OP_RESULT fast_trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback);
   OP_RESULT fast_trained_lookup_new(const KEY key, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT fast_trained_lookup_new(const KEY key);
   OP_RESULT fast_trained_lookup_rmi(const KEY key, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT fast_trained_lookup_rmi(const KEY key);
   virtual OP_RESULT fast_insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length);
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0}) override;
   virtual OP_RESULT scanAscAll(function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   virtual OP_RESULT scanAscAllSeg(function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             function<void()>,
                             u64 rlength = 0) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              function<void()>) override;
   virtual OP_RESULT scanAscSeg(KEY key,
                                u8* start_key,
                                u16 key_length,
                                function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                                function<void()>,
                                u64 rlength = 0) override;
   virtual OP_RESULT scanDescSeg(u8* start_key,
                                 u16 key_length,
                                 function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                                 function<void()>) override;
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;
   void slot_keys(std::vector<KEY>& keys, std::vector<PID>& pids, std::vector<BufferFrame*>& bfs);
   virtual void auto_train(const int maxerror = 0) override;
   virtual void train(const int maxerror) override;
   void train_leaf_nodes(size_t maxerror);
   void train_leaf_nodes_bf(size_t maxerror);
   bool train_leaf_node(HybridPageGuard<BTreeNode>& guard, size_t maxerror);
   void forced_train(const int maxerror) override;
   void fast_train(const int maxerror) override;
   void train_lr() override;
   void scanAll();
   // -------------------------------------------------------------------------------------
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static std::unordered_map<std::string, std::string> serialize(void* btree_object);
   static void deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized);
   static DTRegistry::DTMeta getMeta();
   //  -------------------------------------------
   virtual OP_RESULT attach_spline(HybridPageGuard<BTreeNode>& target_guard, size_t& spline_index, int height);
   virtual OP_RESULT examine_spline_idx(HybridPageGuard<BTreeNode>& target_guard, int height);
   virtual void stats() override;
   OP_RESULT attach_segment(const size_t& segment_ptr);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
