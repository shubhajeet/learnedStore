#pragma once
#include <tbb/concurrent_unordered_map.h>
#include <map>
#include "BTreeInterface.hpp"
#include "BTreeIteratorInterface.hpp"
#include "BTreeNode.hpp"
#include "flat_hash_map.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/compileConst.hpp"
#include "leanstore/lr/learnedIndex.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/rs/builder.hpp"
#include "leanstore/rs/radix_spline.h"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/convert.hpp"
#ifdef INSTRUMENT_CACHE_MISS
#include "cachemisscounter.hpp"
#endif
#ifdef INSTRUMENT_CODE
#include "counter.hpp"
#include "timer.hpp"
#endif
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
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
// -------------------------------------------------------------------------------------
class BTreeGeneric
{
  public:
   // -------------------------------------------------------------------------------------
   template <LATCH_FALLBACK_MODE mode>
   friend class BTreePessimisticIterator;
   // -------------------------------------------------------------------------------------
   // MyNote:: root node of Btree
   BufferFrame* meta_node_bf;  // kept in memory
   atomic<u64> height = 1;
   DTID dt_id;
   spline::RadixSpline<KEY> spline_predictor;
   // rsindex::RadixSpline<KEY> rs_spline_predictor;

#ifdef COMPACT_MAPPING
   std::vector<KEY> mapping_key;
   std::vector<PID> mapping_pid;
   std::vector<BufferFrame*> mapping_bfs;
#else
   std::vector<std::pair<KEY, PID>> secondary_mapping_pid;
   std::vector<std::pair<KEY, BufferFrame*>> secondary_mapping_bf;
   // std::vector<std::pair<KEY, Swip<BTreeNode>>> secondary_mapping_swip;
#endif
   std::string secondary_mapping_file = "secondary_mapping.bin";
   std::string segments_file = "segments.bin";
   std::string attached_segments_file = "attached_segments.bin";
   bool bg_training_thread = false;
   std::thread training_thread;
   std::thread leaf_training_thread;
   std::mutex train_signal_lock;
   std::mutex train_leaf_signal_lock;
   std::shared_mutex model_lock;
   std::condition_variable train_signal;
   std::condition_variable train_leaf_signal;
   bool trained = false;
   int max_error_ = 16;
   int min_attach_level_ = 2;
   ska::flat_hash_map<PID, std::vector<size_t>>& attached_segments = BMC::attached_segments;
   ska::flat_hash_map<PID, spline::RadixSpline<KEY>> leaf_node_segments;
   ska::flat_hash_map<PID, learnedindex<KEY>>& leaf_node_models = BMC::leaf_node_models;
#ifdef SMO_STATS
   int num_splits = 0;
   int incorrect_leaf = 0;
#endif
   // auto& attached_segments = BMC::attached_segments;
   learnedindex<KEY> model;
   // std::map<PID, std::vector<size_t>> attached_segments;
   // std::unordered_map<PID, std::vector<size_t>> attached_segments;
   // tbb::concurrent_unordered_map<PID, std::vector<size_t>> attached_segments;
   // -------------------------------------------------------------------------------------
   BTreeGeneric();
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, BufferFrame* meta_bf);
   // -------------------------------------------------------------------------------------
   bool tryMerge(BufferFrame& to_split, bool swizzle_sibling = true);
   // -------------------------------------------------------------------------------------
   void trySplit(BufferFrame& to_split, s16 pos = -1);
   s16 mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                          s16 left_pos,
                          ExclusivePageGuard<BTreeNode>& from_left,
                          ExclusivePageGuard<BTreeNode>& to_right,
                          bool full_merge_or_nothing);
   enum class XMergeReturnCode : u8 { NOTHING, FULL_MERGE, PARTIAL_MERGE };
   XMergeReturnCode XMerge(HybridPageGuard<BTreeNode>& p_guard, HybridPageGuard<BTreeNode>& c_guard, ParentSwipHandler&);
   // -------------------------------------------------------------------------------------
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
   static ParentSwipHandler findParent(BTreeGeneric& btree_object, BufferFrame& to_find);
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
   static std::unordered_map<std::string, std::string> serialize(BTreeGeneric&);
   static void deserialize(BTreeGeneric&, std::unordered_map<std::string, std::string>);
   // -------------------------------------------------------------------------------------
   ~BTreeGeneric();
   // -------------------------------------------------------------------------------------
   // Mapping key exponential search
   size_t exponentialSearch(const KEY& key, const size_t& pos, const size_t& start, const size_t& end);
   inline BufferFrame* fastTrainedJumpToLeafUsingSegment(const KEY key_int, const size_t segment_id)
   {
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
      static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnStore function: fastTrainedJumpToLeafUsingSegment");
#else
      static auto timer_registry = AvgTimerRegistry<std::string>("learnStore function: fastTrainedJumpToLeafUsingSegment");
#endif
      auto using_segment_timer = timer_registry.registerObject("using_segment", "inference_and_secondary_search");
      Scope scoped_timer(*using_segment_timer);
#endif
      if (!spline_predictor.is_within(key_int, segment_id)) {
         // INFO("key_int: %lu, segment_id: %lu upper_x: %lu upper_pos: %lu lower_x: %lu lower_pos: %lu", key_int, segment_id,
         //      spline_predictor.spline_points_[segment_id - 1].x, spline_predictor.spline_points_[segment_id - 1].y,
         //      spline_predictor.spline_points_[segment_id].x, spline_predictor.spline_points_[segment_id].y);
         return nullptr;
      }
      // #ifdef LATENCY_BREAKDOWN
      // auto inference_timer = timer_registry.registerObject("inference: fastTrainedJumpToLeafUsingSegment ", "inference");
      // inference_timer->start();
      // #endif
      auto pos = spline_predictor.GetEstimatedPosition(key_int, segment_id);
      // INFO("Got pos: %lu for key: %lu", pos, key_int);
      // pos = std::ceil(pos);
      // #ifdef LATENCY_BREAKDOWN
      //       inference_timer->stop();
      // #endif
      // #ifdef LATENCY_BREAKDOWN
      //       auto secondary_search_timer = timer_registry.registerObject("secondary_search: fastTrainedJumpToLeafUsingSegment ",
      //       "secondary_search"); secondary_search_timer->start();
      // #endif
      auto searchbound = spline_predictor.GetSearchBound(pos);
#ifdef COMPACT_MAPPING
#ifdef EXPONENTIAL_SEARCH
      auto leaf_idx = exponentialSearch(key_int, pos, searchbound.begin, searchbound.end);
#else
      auto res = std::lower_bound(mapping_key.begin() + searchbound.begin, mapping_key.begin() + searchbound.end, key_int);
      // auto res = std::lower_bound(mapping_key.begin() + searchbound.begin, mapping_key.begin() + searchbound.end, key_int,
      //                             [](const KEY data, KEY val) { return data < val; });
      auto leaf_idx = std::distance(mapping_key.begin(), res);
#endif
#ifdef DONT_USE_PID
      // auto leaf_idx = binarySearch(mapping_key, pos, max_error_, key_int);
      // auto leaf_idx_simd = binarySearchSIMD(mapping_key, pos, max_error_, key_int);
      // auto leaf_idx = leaf_idx_simd;
      // std::cout << "key: " << key_int << " leaf_idx: " << leaf_idx << " leaf_idx_simd: " << leaf_idx_simd << std::endl;
      // ensure(leaf_idx == leaf_idx_simd);
      // DEBUG_BLOCK()
      // {
      //    if (leaf_idx > 1)
      //       ensure(mapping_key[leaf_idx - 1] <= key_int);
      //    ensure(mapping_key[leaf_idx] >= key_int);
      // }
      // if (leaf_idx >= mapping_bfs.size()) {
      //    INFO("ERROR leaf_idx: %lu, mapping_bfs.size(): %lu", leaf_idx, mapping_bfs.size());
      // }
      // if (leaf_idx >= mapping_pid.size()) {
      //    INFO("ERROR leaf_idx: %lu, mapping_pid.size(): %lu", leaf_idx, mapping_pid.size());
      // }
      auto& lbfs = mapping_bfs[leaf_idx];
      auto lpid = mapping_pid[leaf_idx];
#else
      auto pid = mapping_pid[leaf_idx];
#endif
#else
#ifdef DONT_USE_PID
      auto res = std::lower_bound(secondary_mapping_bf.begin() + searchbound.begin, secondary_mapping_bf.begin() + searchbound.end, key_int,
                                  [](const std::pair<KEY, BufferFrame*>& data, KEY val) { return data.first < val; });
      auto leaf_idx = std::distance(secondary_mapping_bf.begin(), res);
      auto& lbfs = res->second;
      auto lpid = secondary_mapping_pid[leaf_idx].second;
#else
      auto res = std::lower_bound(secondary_mapping_pid.begin() + searchbound.begin, secondary_mapping_pid.begin() + searchbound.end, key_int,
                                  [](const std::pair<KEY, PID>& data, KEY val) { return data.first < val; });
      // auto leaf_idx = std::distance(secondary_mapping_pid.begin(), res);
      // auto pid = secondary_mapping_pid[leaf_idx].second;
      auto pid = res->second;
#endif
#endif
// #ifdef LATENCY_BREAKDOWN
//       secondary_search_timer->stop();
//       auto get_leaf_page_timer = timer_registry.registerObject("get_leaf_page", "get_leaf_page");
//       get_leaf_page_timer->start();
// #endif
#ifdef DONT_USE_PID
      auto bf = lbfs;
      if (bf == nullptr || bf->header.pid != lpid) {
         auto info = BMC::global_bf->getPageinBufferPool(lpid);
         bf = info.bf;
         lbfs = info.bf;
      }
#else
      auto bf = BMC::global_bf->pageInBufferFrame(pid);
      bf = (bf != nullptr) ? bf : BMC::global_bf->getPageinBufferPool(pid);
      DEBUG_BLOCK()
      {
         ensure(bf->header.pid == pid);
      }
#endif
      // #ifdef LATENCY_BREAKDOWN
      //       get_leaf_page_timer->stop();
      // #endif
      return bf;
   };
   bool jumpToLeafUsingSegment(HybridPageGuard<BTreeNode>& target_guard, const KEY key_int, const size_t segment_id);
   inline BufferFrame* jumpToLeafUsingSegment(const KEY key_int, const size_t segment_id)
   {
      if (!spline_predictor.is_within(key_int, segment_id)) {
         return nullptr;
      }
      auto pos = spline_predictor.GetEstimatedPosition(key_int, segment_id);
      auto searchbound = spline_predictor.GetSearchBound(pos);
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
      static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnstore function: jumpToLeafUsingSegment");
#else
      static auto timer_registry = AvgTimerRegistry<std::string>("leanstore_function:jumpToLeafUsingSegment");
#endif
      auto secondary_search_timer = timer_registry.registerObject("secondary search", "secondary_search");
      secondary_search_timer->start();
#endif
#ifdef COMPACT_MAPPING
      // Temporary fix for fast train
      auto res = std::lower_bound(mapping_key.begin() + searchbound.begin, mapping_key.begin() + searchbound.end, key_int,
                                  [](const KEY data, KEY val) { return data <= val; });
      // auto res = std::lower_bound(mapping_key.begin() + searchbound.begin, mapping_key.begin() + searchbound.end, key_int,
      //  [](const KEY data, KEY val) { return data < val; });
      auto leaf_idx = std::distance(mapping_key.begin(), res);
#else
      auto res = std::lower_bound(secondary_mapping_pid.begin() + searchbound.begin, secondary_mapping_pid.begin() + searchbound.end, key_int,
                                  [](const std::pair<KEY, PID>& data, KEY val) { return data.first <= val; });
      auto leaf_idx = std::distance(secondary_mapping_pid.begin(), res);
#endif
#ifdef LATENCY_BREAKDOWN
      secondary_search_timer->stop();
#endif
      // Removing this line just for the sake of the experiment
      leaf_idx = (leaf_idx != 0) ? leaf_idx - 1 : 0;
#ifdef COMPACT_MAPPING
      auto pid = mapping_pid[leaf_idx];
#else
      auto pid = secondary_mapping_pid[leaf_idx].second;
#endif
      auto info = BMC::global_bf->getPageinBufferPool(pid);
      return info.bf;
   }
   // -------------------------------------------------------------------------------------
   // Helpers
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafCanJumpSimulate(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
      target_guard.unlock();
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         auto prob = rand() / static_cast<float>(RAND_MAX);
         if (prob < MISS_PROB) {
            // std::cout << "Reading page from the disk" << std::endl;
            auto pid = target_guard.bf->header.pid;
            BufferFrame testbf;
            BMC::global_bf->getPage(pid, &testbf);
         }
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      auto prob = rand() / static_cast<float>(RAND_MAX);
      if (prob < MISS_PROB) {
         // std::cout << "Reading page from the disk" << std::endl;
         auto pid = target_guard.bf->header.pid;
         BufferFrame testbf;
         BMC::global_bf->getPage(pid, &testbf);
      }
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafCanJump(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
#ifdef INSTRUMENT_CODE
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
      static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>(
          "modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search");
#else
      static auto timer_registry = AvgTSCTimerRegistry<size_t>(
          "modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search");
#endif
#else
#ifdef SIMPLE_SIZE
      static auto timer_registry = SimpleAvgTimerRegistry<SIMPLE_SIZE>(
          "modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search");
#else
      static auto timer_registry = AvgTimerRegistry<size_t>(
          "modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search");
#endif
#endif
      auto find_leaf_timer = timer_registry.registerObject(1, " level: total_innner_search");
      Scope functionTimer(*find_leaf_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
      static auto cache_registry = AvgCacheMissCounterRegistry(
          "modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) + " purpose: benchmark");
      auto find_leaf_cache = cache_registry.registerObject(1, "findLeaf");
      Scope function_cache_miss(*find_leaf_cache);
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
      static auto timer_registry =
          SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) +
                                                 " level: total_inner_search purpose: latency_breakdown");
#else
      static auto timer_registry =
          AvgTSCTimerRegistry<size_t>("modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) +
                                      " level: total_inner_search purpose: latency_breakdown");
#endif
#else
#ifdef SIMPLE_SIZE
      static auto timer_registry =
          SimpleAvgTimerRegistry<SIMPLE_SIZE>("modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) +
                                              " level: total_inner_search purpose: latency_breakdown");
#else
      static auto timer_registry =
          AvgTimerRegistry<size_t>("modified_leanstore func: findLeafCanJump mode: " + std::to_string(static_cast<int>(mode)) +
                                   " level: total_inner_search purpose: latency_breakdown");
#endif
#endif
      auto other_timer = timer_registry.registerObject(height, "findLeafOther");
      other_timer->start();
#endif
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
#ifdef LATENCY_BREAKDOWN
      other_timer->stop();
#endif
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
#ifdef LATENCY_BREAKDOWN
         auto levelTimer = timer_registry.registerObject(level, "inner_node_search");
         Scope inner_node_timer(*levelTimer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto levelMC = cache_registry.registerObject(level, "inner_node_search");
         Scope inner_node_cache_miss(*levelMC);
#endif
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafParentCanJump(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         if (level == (height - 2)) {
            // INFO("findLeafParentCanJump found parent ");
            p_guard.unlock();
            return;
         }
         p_guard = std::move(target_guard);
         if (level == (height - 1)) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }

   inline BufferFrame* fastTrainFindLeafUsingSegmentAttachedAtRoot(KEY key)
   {
      // if (!spline_predictor.WithinSpline(key)) {
      //    return nullptr;
      // }
      auto spline_idx = spline_predictor.GetSplineSegment(key);
      BufferFrame* found = fastTrainedJumpToLeafUsingSegment(key, spline_idx);
      return found;
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void fastTrainFindLeafUsingSegment(HybridPageGuard<BTreeNode>& target_guard, KEY key, const u8* key_bytes)
   {
#ifdef INSTRUMENT_CODE
#ifdef USE_TSC
      static auto timer_registry =
          AvgTSCTimerRegistry<std::string>("learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)));
      auto find_leaf_timer = timer_registry.registerObject(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search",
          "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#else
      static auto timer_registry =
          AvgTimerRegistry<std::string>("learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)));
      auto find_leaf_timer = timer_registry.registerObject(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " level: total_inner_search",
          "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#endif
#endif
#ifdef INSTRUMENT_CACHE_MISS
      static auto cache_registry = AvgCacheMissCounterRegistry(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: benchmark");
      auto find_leaf_cache = cache_registry.registerObject(0, "findLeaf");
      Scope function_cache_miss(*find_leaf_cache);
#endif
#ifdef SEGMENT_STATS
      static auto counter_registry = CounterRegistry();
      auto used_segments_counter =
          counter_registry.registerObject("learnstore func: fastTrainFindLeafUsingSegment counter: total_segment_used", "segment_used");
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
      static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: latency_breakdown");
#else
      static auto timer_registry = AvgTSCTimerRegistry<std::string>(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: latency_breakdown");
#endif
#else
#ifdef SIMPLE_SIZE
      static auto timer_registry = SimpleAvgTimerRegistry<SIMPLE_SIZE>(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: latency_breakdown");
#else
      static auto timer_registry = AvgTimerRegistry<std::string>(
          "learnstore func: fastTrainFindLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: latency_breakdown");
#endif
#endif
      // auto other_timer = timer_registry.registerObject("other part of find leaf", "findLeafOther");
      // other_timer->start();
#endif
      const auto key_length = sizeof(KEY);
      target_guard.unlock();
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
// -------------------------------------------------------------------------------------
// for loop to till min_attach_level_ to search for segment
#ifdef LATENCY_BREAKDOWN
      auto find_spline_seg_timer = timer_registry.registerObject("find_spline_segment", "find_spline_segment");
      find_spline_seg_timer->start();
#endif
      int16_t check_to_level = height - min_attach_level_;
      for (; level < check_to_level && !target_guard->is_leaf; level++) {
#ifdef LATENCY_BREAKDOWN
         // auto levelTimer = timer_registry.registerObject("inner_node_search level: " + std::to_string(level), "inner_node_search");
         // Scope inner_node_timer(*levelTimer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto levelMC = cache_registry.registerObject(level, "inner_node_search");
         Scope inner_node_cache_miss(*levelMC);
#endif
#ifdef LATENCY_BREAKDOWN
         // auto additional_block_timer = timer_registry.registerObject("level: " + std::to_string(level + 1), "total_extra_work");
         // additional_block_timer->start();
#endif
#ifdef LATENCY_BREAKDOWN
         // auto attached_segments_lookup = timer_registry.registerObject("mzr: hash_lookup_overhead", "attached segment lookup");
         // attached_segments_lookup->start();
#endif
         PID pid = target_guard.bf->header.pid;
         auto seg_itr = attached_segments.find(pid);
#ifdef LATENCY_BREAKDOWN
         // attached_segments_lookup->stop();
#endif
         // auto& segments = target_guard.bf->header.attached_segments;

         if (seg_itr != attached_segments.end()) {
            // if (segments != nullptr) {
            auto& seg_ptr_vec = seg_itr->second;
            // TODO:: search for the correct segment
            // assert(target_guard.bf->header.attached_segments != nullptr);
            // assert(target_guard.bf->header.attached_segments == &(BMC::attached_segments[target_guard.bf->header.pid]));
#ifdef LATENCY_BREAKDOWN
            // static auto segment_search_timer =
            //  timer_registry.registerObject(" mzr: segment_search_overhead pid: " + std::to_string(pid), "segment_search");
            // segment_search_timer->start();
#endif
            // auto it = std::lower_bound(segments->begin(), segments->end(), key,
            //                            [&](size_t a, KEY b) { return spline_predictor.spline_points_[a].x < key; });
            auto it = std::lower_bound(seg_ptr_vec.begin(), seg_ptr_vec.end(), key,
                                       [&](size_t a, KEY b) { return spline_predictor.spline_points_[a].x < b; });
#ifdef LATENCY_BREAKDOWN
            // segment_search_timer->stop();
#endif
            if (it != seg_ptr_vec.end()) {
               // if (it != segments->end()) {

#ifdef LATENCY_BREAKDOWN
               find_spline_seg_timer->stop();
#endif
               auto found = fastTrainedJumpToLeafUsingSegment(key, *it);
               if (found != nullptr) {
                  auto leaf = HybridPageGuard<BTreeNode>(found);
#ifdef SEGMENT_STATS
                  auto jump_level_counter =
                      counter_registry.registerObject("learnstore func:fastTrainFindLeafUsingSegment level: " + std::to_string(level) +
                                                          " mode: " + std::to_string(static_cast<int>(mode)) + " mzr: jump_level",
                                                      "jump_level");
                  jump_level_counter->count();
                  used_segments_counter->count();
#endif
                  // s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
                  // if (sanity_check_result == 0) {
                  //    target_guard = std::move(leaf);
                  //    return;
                  // } else {
                  //    break;
                  // }
                  target_guard = std::move(leaf);
                  return;
               } else {
               }
            }
         }
#ifdef LATENCY_BREAKDOWN
         // additional_block_timer->stop();
#endif
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key_bytes, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
      }

      while (!target_guard->is_leaf) {
#ifdef LATENCY_BREAKDOWN
         auto levelTimer = timer_registry.registerObject("inner_node_search level: " + std::to_string(level), "inner_node_search");
         Scope inner_node_timer(*levelTimer);
#endif
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key_bytes, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
#ifdef LATENCY_BREAKDOWN
      find_spline_seg_timer->stop();
#endif
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafUseSegmentInDisk(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         auto prob = rand() / static_cast<double>(RAND_MAX);
         if (prob < MISS_PROB) {
            // convert key to integer or number
            auto key_int = utils::u8_to<KEY>(key, key_length);
            // search for the segment
            auto pred = spline_predictor.GetEstimatedPosition(key_int);
            // load the leaf and return
            if (pred >= 0) {
               auto found = jumpToLeafUsingSegment(target_guard, key_int, pred);
               if (found) {
                  auto prob = rand() / static_cast<float>(RAND_MAX);
                  if (prob < MISS_PROB) {
                     // std::cout << "Reading page from the disk" << std::endl;
                     auto pid = target_guard.bf->header.pid;
                     BufferFrame testbf;
                     BMC::global_bf->getPage(pid, &testbf);
                  }
                  return;
               }
            }
         }
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      auto prob = rand() / static_cast<float>(RAND_MAX);
      if (prob < MISS_PROB) {
         // std::cout << "Reading page from the disk" << std::endl;
         auto pid = target_guard.bf->header.pid;
         BufferFrame testbf;
         BMC::global_bf->getPage(pid, &testbf);
      }
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafUsingSegment(HybridPageGuard<BTreeNode>& target_guard, KEY key, const u8* key_bytes)
   {
      const auto key_length = sizeof(KEY);
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
      static auto timer_registry =
          AvgTSCTimerRegistry<std::string>("learnstore func: findLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)));
      auto find_leaf_timer = timer_registry.registerObject("findLeafUsingSegment", "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#else
      static auto timer_registry =
          AvgTimerRegistry<std::string>("learnstore func: findLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)));
      auto find_leaf_timer = timer_registry.registerObject("findLeafUsingSegment", "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#endif
#endif
#ifdef INSTRUMENT_CACHE_MISS
      static auto cache_registry = AvgCacheMissCounterRegistry(
          "learnstore func: findLeafUsingSegment mode: " + std::to_string(static_cast<int>(mode)) + " purpose: benchmark");
      auto find_leaf_cache = cache_registry.registerObject(0, "findLeaf");
      Scope function_cache_miss(*find_leaf_cache);
#endif
#ifdef SEGMENT_STATS
      static auto counter_registry = CounterRegistry();
      auto used_segments_counter =
          counter_registry.registerObject("learnstore func: findLeafUsingSegment counter: total_segment_used", "segment_used");
#endif
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
#ifdef LATENCY_BREAKDOWN
         auto levelTimer = timer_registry.registerObject("level: " + std::to_string(level), "inner_node_search");
         Scope inner_node_timer(*levelTimer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto levelMC = cache_registry.registerObject(level, "inner_node_search");
         Scope inner_node_cache_miss(*levelMC);
#endif
         // Mynote: looks for the child pointer
         // TODO :: Look for the key in the attached spline segments

         if (level < (height - min_attach_level_)) {
#ifdef LATENCY_BREAKDOWN
            auto additional_block_timer = timer_registry.registerObject("level: " + std::to_string(level), "total_extra_work");
            additional_block_timer->start();
#endif
#ifdef LATENCY_BREAKDOWN

            auto attached_segments_lookup = timer_registry.registerObject("mzr: hash_lookup_overhead", "attached segment lookup");
            attached_segments_lookup->start();
#endif
            PID pid = target_guard.bf->header.pid;
            auto seg_itr = attached_segments.find(pid);
#ifdef LATENCY_BREAKDOWN
            attached_segments_lookup->stop();
#endif
            if (seg_itr != attached_segments.end()) {
               auto& seg_ptr_vec = seg_itr->second;
// TODO:: search for the correct segment
#ifdef LATENCY_BREAKDOWN
               static auto segment_search_timer =
                   timer_registry.registerObject(" mzr: segment_search_overhead pid: " + std::to_string(pid), "segment_search");
               segment_search_timer->start();
#endif
               auto it = std::lower_bound(seg_ptr_vec.begin(), seg_ptr_vec.end(), key,
                                          [&](size_t a, KEY b) { return spline_predictor.spline_points_[a].x < key; });
#ifdef LATENCY_BREAKDOWN
               segment_search_timer->stop();
#endif
               if (it != seg_ptr_vec.end()) {
                  // p_guard = std::move(target_guard);
                  // auto found = jumpToLeafUsingSegment(target_guard, key, *it);
                  auto found = jumpToLeafUsingSegment(key, *it);
                  if (found != nullptr) {
                     target_guard = HybridPageGuard<BTreeNode>(found);
                     // check if the bf is valid
                     // auto test_guard = HybridPageGuard<BTreeNode>(found);
                     // s16 sanity_check_result = test_guard->compareKeyWithBoundaries(key_bytes, key_length);
                     // if (sanity_check_result == 0) {
                     // target_guard = std::move(test_guard);
#ifdef SEGMENT_STATS
                     auto jump_level_counter =
                         counter_registry.registerObject("learnstore func: findLeafUsingSegment level: " + std::to_string(level) +
                                                             " mode: " + std::to_string(static_cast<int>(mode)) + " mzr: jump_level",
                                                         "jump_level");
                     jump_level_counter->count();
                     used_segments_counter->count();
#endif
                     return;
                     // }
                     // std::cout << "jump to leaf success" << std::endl;
                  } else {
                     // std::cout << "jump to leaf failed" << std::endl;
                  }
               }
            }
            Swip<BTreeNode>& c_swip = target_guard->lookupInner(key_bytes, key_length);
            p_guard = std::move(target_guard);
            if (level == height - 1) {
               target_guard = HybridPageGuard(p_guard, c_swip, mode);
            } else {
               target_guard = HybridPageGuard(p_guard, c_swip);
            }
            level++;
         }
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findFirstLeafParentCanJump(HybridPageGuard<BTreeNode>& target_guard)
   {
      target_guard.unlock();
      // Mynote: points to upper node
      // INFO("find first leaf parent can jump");
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         // Mynote: looks for the child pointer
         // Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         Swip<BTreeNode>& c_swip = target_guard->getChild(0);
         if (level == (height - 2)) {
            // Display all the slot keys
            // for (u16 i = 0; i < target_guard->count; i++) {
            //    auto key_len = target_guard->getKeyLen(i);
            //    u8 key_bytes[key_len];
            //    target_guard->copyFullKey(i, key_bytes);
            //    auto int_key = utils::u8_to<KEY>(key_bytes, key_len);
            //    std::cout << i << " " << int_key << std::endl;
            // }
            // auto lower_fence_key = target_guard->getLowerFenceKey();
            // auto lower_fence_key_len = target_guard->lower_fence.length;
            // auto upper_fence_key = target_guard->getUpperFenceKey();
            // auto upper_fence_key_len = target_guard->upper_fence.length;
            // auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
            // auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
            // std::cout << "lower_fence_key: " << lower_fence_key_int << " upper_fence_key: " << upper_fence_key_int << std::endl;
            p_guard.unlock();
            // INFO("Found the first leaf parent");
            return;
         }
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findFirstLeafCanJump(HybridPageGuard<BTreeNode>& target_guard)
   {
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         // Mynote: looks for the child pointer
         // Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         Swip<BTreeNode>& c_swip = target_guard->getChild(0);
         p_guard = std::move(target_guard);
         // if (level == (height - 2)) {
         //    // Display all the slot keys
         //    for (u16 i = 0; i < target_guard->count; i++) {
         //       auto key_len = target_guard->getKeyLen(i);
         //       u8 key_bytes[key_len];
         //       target_guard->copyFullKey(i, key_bytes);
         //       auto int_key = utils::u8_to<KEY>(key_bytes, key_len);
         //       std::cout << i << " " << int_key << std::endl;
         //    }
         //    auto lower_fence_key = target_guard->getLowerFenceKey();
         //    auto lower_fence_key_len = target_guard->lower_fence.length;
         //    auto upper_fence_key = target_guard->getUpperFenceKey();
         //    auto upper_fence_key_len = target_guard->upper_fence.length;
         //    auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         //    auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
         //    keys.push_back(lower_fence_key_int);
         //    std::cout << "lower_fence_key: " << lower_fence_key_int << " upper_fence_key: " << upper_fence_key_int << std::endl;
         // }
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);

         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   // inline void findLeafUsingSegment(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length, BufferFrame* bfptr)
   inline void findLeafUsingSegment(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
      static auto timer_registry = AvgTSCTimerRegistry<std::string>();
      auto find_leaf_timer =
          timer_registry.registerObject("learnedstore mode: " + std::to_string(static_cast<int>(mode)) + " func: findLeafUsingSegment", "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#else
      static auto timer_registry = AvgTimerRegistry<std::string>();
      auto find_leaf_timer =
          timer_registry.registerObject("learnstore mode: " + std::to_string(static_cast<int>(mode)) + " func: findLeafUsingSegment", "findLeaf");
      Scope functionTimer(*find_leaf_timer);
#endif
#endif
#ifdef INSTRUMENT_CACHE_MISS
      static auto cache_registry =
          AvgCacheMissCounterRegistry<std::string>("learnstore mode: " + std::to_string(static_cast<int>(mode)) + " purpose: benchmark");
      auto find_leaf_cache = cache_registry.registerObject(0, "findLeaf");
      Scope function_cache_miss(*find_leaf_cache);
#endif
#ifdef SEGMENT_STATS
      static auto counter_registry = CounterRegistry();
      auto used_segments_counter = counter_registry.registerObject("learnstore counter: total_segment_used", "segment_used");
#endif
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      KEY key_int = utils::u8_to<KEY>(key, key_length);
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
#ifdef LATENCY_BREAKDOWN
         auto levelTimer =
             timer_registry.registerObject("inner node level: " + std::to_string(level) + " func: findLeafUsingSegment", "inner_node_search");
         // auto levelTimer = timer_registry.registerObject(level, "inner_node_search");
         Scope inner_node_timer(*levelTimer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto levelMC = cache_registry.registerObject("inner node level: " + std::to_string(level), "inner_node_search");
         Scope inner_node_cache_miss(*levelMC);
#endif
// Mynote: looks for the child pointer
// TODO :: Look for the key in the attached spline segments
#ifndef AVOID_SEGMENT_SEARCH
         if (level < (height - min_attach_level_)) {
#ifdef LATENCY_BREAKDOWN
            auto additional_block_timer = timer_registry.registerObject(
                "additional block counter level: " + std::to_string(level + 1) + "func: findLeafUsingSegment", "total_extra_work");
            additional_block_timer->start();
#endif
#ifdef LATENCY_BREAKDOWN

            auto attached_segments_lookup = timer_registry.registerObject(
                "learnstore mode: " + std::to_string(static_cast<int>(mode)) + "mzr: hash_lookup_overhead func: findLeafUsingSegment",
                "attached segment lookup");
            attached_segments_lookup->start();
#endif
            PID pid = target_guard.bf->header.pid;
            auto seg_itr = attached_segments.find(pid);
#ifdef LATENCY_BREAKDOWN
            attached_segments_lookup->stop();
#endif
            if (seg_itr != attached_segments.end()) {
               auto& seg_ptr_vec = seg_itr->second;
// TODO:: search for the correct segment
#ifdef LATENCY_BREAKDOWN
               static auto segment_search_timer =
                   timer_registry.registerObject("learnedstore mode: " + std::to_string(static_cast<int>(mode)) +
                                                     " func: findLeafUsingSegment mzr: segment_search_overhead pid: " + std::to_string(pid),
                                                 "segment_search");
               segment_search_timer->start();
#endif
               auto it = std::lower_bound(seg_ptr_vec.begin(), seg_ptr_vec.end(), key_int,
                                          [&](size_t a, KEY b) { return spline_predictor.spline_points_[a].x < key_int; });
#ifdef LATENCY_BREAKDOWN
               segment_search_timer->stop();
#endif
               if (it != seg_ptr_vec.end()) {
                  // p_guard = std::move(target_guard);
                  auto found = jumpToLeafUsingSegment(target_guard, key_int, *it);
                  if (found) {
#ifdef SEGMENT_STATS
                     auto jump_level_counter = counter_registry.registerObject("learnstore level: " + std::to_string(level) +
                                                                                   " mode: " + std::to_string(static_cast<int>(mode)) +
                                                                                   " func: findLeafUsingSegment mzr: jump_level",
                                                                               "jump_level");
                     jump_level_counter->count();
                     used_segments_counter->count();
#endif
                     return;
                  } else {
                  }
               }
            }
#ifdef LATENCY_BREAKDOWN
            additional_block_timer->stop();
#endif
         }
#endif
//
#ifdef AVOID_SEGMENT_SEARCH
         s32 pos = target_guard->lowerBound<false>(key, key_length);

         if (pos != target_guard->count) {
            // MyNote: why is it returning upper. This does not make sense.
            // MyNote: should have logical interaction with buffer manager
            PID pid = target_guard.bf->header.pid;
            auto seg_itr = attached_segments.find(pid);
            if (seg_itr != attached_segments.end()) {
               auto& seg_ptr_vec = seg_itr->second;
               if (seg_ptr_vec[pos] != 0) {
                  p_guard = std::move(target_guard);
                  auto found = jumpToLeafUsingSegment(target_guard, key_int, seg_ptr_vec[pos]);
                  if (found) {
#ifdef SEGMENT_STATS
                     jump_level_counter.countEvent(level);
                     used_segments_counter.countEvent(true);
#endif
                     return;
                  }
               }
            }
         }
         Swip<BTreeNode>& c_swip = (pos == target_guard->count) ? target_guard->upper : target_guard->getChild(pos);
#else
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
#endif
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }

   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLastLeafCanJump(HybridPageGuard<BTreeNode>& target_guard)
   {
      target_guard.unlock();
      // Mynote: points to upper node
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         // Mynote: looks for the child pointer
         // Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         Swip<BTreeNode>& c_swip = target_guard->getChild(target_guard->count);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }
   // -------------------------------------------------------------------------------------
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findLeafAndLatch(HybridPageGuard<BTreeNode>& target_guard, const u8* key, u16 key_length)
   {
      u32 volatile mask = 1;
      while (true) {
         jumpmuTry()
         {
            findLeafCanJump<mode>(target_guard, key, key_length);
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch()
         {
            BACKOFF_STRATEGIES()
         }
      }
   }
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findFirstLeafAndLatch(HybridPageGuard<BTreeNode>& target_guard)
   {
      u32 volatile mask = 1;
      while (true) {
         jumpmuTry()
         {
            findFirstLeafCanJump<mode>(target_guard);
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch()
         {
            BACKOFF_STRATEGIES()
         }
      }
   }
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findFirstLeafParentAndLatch(HybridPageGuard<BTreeNode>& target_guard)
   {
      u32 volatile mask = 1;
      while (true) {
         jumpmuTry()
         {
            findFirstLeafParentCanJump<mode>(target_guard);
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch()
         {
            BACKOFF_STRATEGIES()
         }
      }
   }
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findLastLeafAndLatch(HybridPageGuard<BTreeNode>& target_guard)
   {
      u32 volatile mask = 1;
      while (true) {
         jumpmuTry()
         {
            findLastLeafCanJump<mode>(target_guard);
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch()
         {
            BACKOFF_STRATEGIES()
         }
      }
   }
   // -------------------------------------------------------------------------------------
   // Helpers
   // -------------------------------------------------------------------------------------
   inline bool isMetaNode(HybridPageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf; }
   inline bool isMetaNode(ExclusivePageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf(); }
   s64 iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   s64 iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   s64 iterateAllPagesWithoutCheck(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf, LATCH_FALLBACK_MODE mode);
   s64 iterateAllPagesRecWithoutCheck(HybridPageGuard<BTreeNode>& node_guard,
                                      std::function<s64(BTreeNode&)> inner,
                                      std::function<s64(BTreeNode&)> leaf,
                                      LATCH_FALLBACK_MODE mode);
   u64 countInner();
   u64 countPages();
   u64 countEntries();
   u64 getHeight();
   double averageSpaceUsage();
   u32 bytesFree();
   bool learnedIndexLoad();
   bool learnedIndexStore();
   void printInfos(uint64_t totalSize);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
