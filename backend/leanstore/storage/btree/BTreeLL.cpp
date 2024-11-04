#include "BTreeLL.hpp"
#include "core/BTreeGenericIterator.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/fold.hpp"
// #include "leanstore/BTreeAdapter.hpp"
// #include "leanstore/utils/convert.hpp"
#ifdef INSTURMENT_CODE
#include "cachemisscounter.hpp"
#include "functiontimer.hpp"
#include "timer.hpp"
#endif
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
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
OP_RESULT BTreeLL::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
#ifdef INSTRUMENT_CODE
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("modified_leanstore func: lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("modified_leanstore func: lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("modified_leanstore func: lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("modified_leanstore func: lookup purpose: benchmark");
#endif
#endif
   auto lookup_timer = timer_registry.registerObject(0, "total_lookup");
   Scope functionTimer(*lookup_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
   static auto cache_registry = AvgCacheMissCounterRegistry("modified_leanstore func: lookup purpose: cache_miss");
   auto lookup_cache_miss = cache_registry.registerObject(0, "total_lookup");
   Scope functionCacheMiss(*lookup_cache_miss);
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("modified_leanstore func: lookup purpose: latency_breakdown");
#else
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("modified_leanstore func: lookup level: purpose: latency_breakdown");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTimerRegistry<SIMPLE>("modified_leanstore func: lookup purpose: latency_breakdown");
#else
   static auto timer_registry = AvgTimerRegistry<size_t>("modified_leanstore func: lookup purpose: latency_breakdown");
#endif
#endif
   // auto other_timer = timer_registry.registerObject(height + 3, "lookup");
   // other_timer->start();
#endif
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, key, key_length);
         // #ifdef LATENCY_BREAKDOWN
         //          other_timer->start();
         // #endif
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key, key_length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
            }
            ensure(sanity_check_result == 0);
         }
// -------------------------------------------------------------------------------------
// Mynote: Guess find key in leaf node
#ifdef LATENCY_BREAKDOWN
         auto leaf_search_timer = timer_registry.registerObject(height + 1, "leaf_search_lookup");
         leaf_search_timer->start();
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto leaf_search_cache_miss = cache_registry.registerObject(height + 1, "leaf_search_lookup");
         leaf_search_cache_miss->start();
#endif
         s16 pos = leaf->lowerBound<true>(key, key_length);
#ifdef INSTRUMENT_CACHE_MISS
         leaf_search_cache_miss->stop();
#endif
#ifdef LATENCY_BREAKDOWN
         leaf_search_timer->stop();
#endif
         if (pos != -1) {
#ifdef LATENCY_BREAKDOWN
            auto payload_copy_timer = timer_registry.registerObject(height + 2, "payload_copy_lookup");
            Scope payload_timer_scoped(*payload_copy_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
            auto payload_cache_miss = cache_registry.registerObject(height + 2, "payload_copy_lookup");
            Scope payload_cache_scoped(*payload_cache_miss);
#endif
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
            // raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}

OP_RESULT BTreeLL::lookup_simulate_long_tail(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         // Mynote: Goes down the tree to the leafnode
         findLeafCanJumpSimulate(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key, key_length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
            }
            ensure(sanity_check_result == 0);
         }
         // -------------------------------------------------------------------------------------
         // Mynote: Guess find key in leaf node
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
            // raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}

// OP_RESULT BTreeLL::fast_tail_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
// {
// #ifdef INSTRUMENT_CODE
// #ifdef USE_TSC
// #ifdef SIMPLE_SIZE
//    static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_tail_lookup purpose: benchmark");
// #else
//    static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: fast_tail_lookup purpose: benchmark");
// #endif
// #else
// #ifdef SIMPLE_SIZE
//    sta static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: fast_tail_lookup purpose: benchmark");
// #else
//    static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: fast_tail_lookup purpose: benchmark");
// #endif
// #endif
//    auto lookup_timer = timer_registry.registerObject(0, "total_lookup");
//    Scope functionTimer(*lookup_timer);
// #endif
// #ifdef INSTRUMENT_CACHE_MISS
//    static auto cache_registry = AvgCacheMissCounterRegistry("learnstore func: fast_tail_lookup purpose: cache_miss");
//    auto lookup_cache_miss = cache_registry.registerObject(0, "total_lookup");
//    Scope functionCacheMiss(*lookup_cache_miss);
// #endif
// #ifdef LATENCY_BREAKDOWN
// #ifdef USE_TSC
// #ifdef SIMPLE_SIZE
//    static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_tail_lookup purpose: latency_breakdown");
// #else
//    static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: fast_tail_lookup level: purpose: latency_breakdown");
// #endif
// #else
// #ifdef SIMPLE_SIZE
//    static auto timer_registry = SimpleAvgTimerRegistry<SIMPLE>("learnstore func: fast_tail_lookup purpose: latency_breakdown");
// #else
//    static auto timer_registry = AvgTimerRegistry<size_t>("learnstore func: fast_tail_lookup purpose: latency_breakdown");
// #endif
// #endif
//    // auto other_timer = timer_registry.registerObject(height + 3, "lookup");
//    // other_timer->start();
// #endif
//    volatile u32 mask = 1;
//    while (true) {
//       jumpmuTry()
//       {
//          HybridPageGuard<BTreeNode> leaf;
//          findLeafUseSegmentInDisk(leaf, key, key_length);
//          // #ifdef LATENCY_BREAKDOWN
//          //          other_timer->start();
//          // #endif
//          // -------------------------------------------------------------------------------------
//          DEBUG_BLOCK()
//          {
//             s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
//             leaf.recheck();
//             if (sanity_check_result != 0) {
//                auto key_int = utils::u8_to<KEY>(key, key_length);
//                auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
//                auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
//                cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
//                     << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
//             }
//             ensure(sanity_check_result == 0);
//          }
// // -------------------------------------------------------------------------------------
// // Mynote: Guess find key in leaf node
// #ifdef LATENCY_BREAKDOWN
//          auto leaf_search_timer = timer_registry.registerObject(height + 1, "leaf_search_lookup");
//          leaf_search_timer->start();
// #endif
// #ifdef INSTRUMENT_CACHE_MISS
//          auto leaf_search_cache_miss = cache_registry.registerObject(height + 1, "leaf_search_lookup");
//          leaf_search_cache_miss->start();
// #endif
//          s16 pos = leaf->lowerBound<true>(key, key_length);
// #ifdef INSTRUMENT_CACHE_MISS
//          leaf_search_cache_miss->stop();
// #endif
// #ifdef LATENCY_BREAKDOWN
//          leaf_search_timer->stop();
// #endif
//          if (pos != -1) {
// #ifdef LATENCY_BREAKDOWN
//             auto payload_copy_timer = timer_registry.registerObject(height + 2, "payload_copy_lookup");
//             Scope payload_timer_scoped(*payload_copy_timer);
// #endif
// #ifdef INSTRUMENT_CACHE_MISS
//             auto payload_cache_miss = cache_registry.registerObject(height + 2, "payload_copy_lookup");
//             Scope payload_cache_scoped(*payload_cache_miss);
// #endif
//             payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
//             leaf.recheck();
//             jumpmu_return OP_RESULT::OK;
//          } else {
//             leaf.recheck();
//             // raise(SIGTRAP);
//             jumpmu_return OP_RESULT::NOT_FOUND;
//          }
//       }
//       jumpmuCatch()
//       {
//          BACKOFF_STRATEGIES()
//          WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
//       }
//    }
// }
OP_RESULT BTreeLL::fast_tail_lookup(u8* key_bytes, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   auto key = utils::u8_to<KEY>(key_bytes, key_length);
   if (std::shared_lock<std::shared_mutex> lock(model_lock);
       trained && lock.owns_lock() && mapping_key[0] <= key && key <= mapping_key[mapping_key.size() - 1]) {
      // std::cout << "Using segment" << std::endl;
      auto spline_idx = spline_predictor.GetSplineSegment(key);
      auto leaf_idx = spline_predictor.GetEstimatedPosition(key, spline_idx, mapping_key);
      BufferFrame* leaf_bf = mapping_bfs[leaf_idx];
      // Simulate disk access with probability
      auto prob = rand() / static_cast<float>(RAND_MAX);
      if (prob < MISS_PROB) {
         // std::cout << "Reading page from the disk" << std::endl;
         auto pid = mapping_pid[leaf_idx];
         BufferFrame testbf;
         BMC::global_bf->getPage(pid, &testbf);
      }
      // end of simulation code

#ifdef PID_CHECK
      if (auto pid = mapping_pid[leaf_idx]; leaf_bf == nullptr || leaf_bf->header.pid != pid) {
         auto info = BMC::global_bf->getPageinBufferPool(pid);
         leaf_bf = info.bf;
         mapping_bfs[leaf_idx] = leaf_bf;
      }
#endif
      // BufferFrame* leaf_bf = fastTrainFindLeafUsingSegmentAttachedAtRoot(key);
      if (leaf_bf != nullptr) {
         HybridPageGuard<BTreeNode> leaf(leaf_bf);
         s16 pos = 0;
#ifdef MODEL_IN_LEAF_NODE
#ifdef MODEL_LR
         auto swip = leaf.swip();
         auto bf = swip.bfPtr();
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         if (auto& model = bf->header.model; model.m == 0) {
            auto predict = bf->header.model.predict(key);
#ifdef EXPONENTIAL_SEARCH
            pos = leaf->exponentialSearch(key_bytes, key_length, predict);
#else
            auto search_bound = bf->header.model.get_searchbound(predict, leaf->count);
            pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
#endif
            //          auto leaf_model = leaf_node_models.find(leaf_bf->header.pid);
            //          if (leaf_model != leaf_node_models.end()) {
            //             // INFO("Found model in leaf node");
            //             auto predict = leaf_model->second.predict(key);
            // // INFO("Predicted position %d", predict);
            // #ifdef EXPONENTIAL_SEARCH
            //             pos = leaf->exponentialSearch(key_bytes, key_length, predict);
            // #else
            //             auto search_bound = leaf_model->second.get_searchbound(predict, leaf->count);
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
            //             pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
            // #endif
            //             // INFO("Found position %d", pos);
            //          } else {
            //             std::cout << "All leaf node should have models attached to it" << std::endl;
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             pos = leaf->lowerBound<true>(key_bytes, key_length);
            //          }
         } else {
            pos = leaf->lowerBound<true>(key_bytes, key_length);
         }
#else
         auto spline_idx = leaf_bf->header.splines.GetSplineSegment(key);
         auto predict = leaf_bf->header.splines.GetEstimatedPosition(key, spline_idx);
         auto search_bound = leaf_bf->header.splines.GetSearchBound(predict);
         pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         // auto leaf_model = leaf_node_segments.find(leaf_bf->header.pid);
         // if (leaf_model != leaf_node_segments.end()) {
         //    // INFO("Found model in leaf node");
         //    auto spline_idx = leaf_model->second.GetSplineSegment(key);
         //    // INFO("Spline index %d", spline_idx);
         //    auto predict = leaf_model->second.GetEstimatedPosition(key, spline_idx);
         //    // INFO("Predicted position %d", predict);
         //    auto search_bound = leaf_model->second.GetSearchBound(predict);
         //    // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
         //    pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         //    // INFO("Found position %d", pos);
         // } else {
         //    pos = leaf->lowerBound<true>(key_bytes, key_length);
         // }
#endif
#else
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         pos = leaf->lowerBound<true>(key_bytes, key_length);
#endif
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            // INFO("4 leaf_bf: %p key: %lu", leaf_bf, key);
            return OP_RESULT::OK;
         } else {
            auto key_length = sizeof(KEY);
            u8 key_bytes[key_length];
            fold(key_bytes, key);
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            if (sanity_check_result == 0) {
               return OP_RESULT::NOT_FOUND;
            } else {
#ifdef SMO_STATS
               incorrect_leaf++;
               train_signal.notify_one();
#endif
            }
         }
      }
   }
   // auto key_length = sizeof(KEY);
   // u8 key_bytes[key_length];
   // fold(key_bytes, key);
   return lookup_simulate_long_tail(key_bytes, key_length, payload_callback);
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT BTreeLL::trained_lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
#ifdef INSTRUMENT_CODE
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<size_t>("learnstore func: trained_lookup purpose: benchmark");
#endif
#endif
   auto lookup_timer = timer_registry.registerObject(0, "lookup");
   Scope functionTimer(*lookup_timer);
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnstore func: trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<std::string>("learnstore func: fast_trained_lookup purpose: benchmark");
#endif
#endif
#endif
#ifdef INSTRUMENT_CACHE_MISS
   static auto cache_registry = AvgCacheMissCounterRegistry("learnstore func: trained_lookup purpose: benchmark");
   auto lookup_cache_miss = cache_registry.registerObject(0, "total_lookup");
   Scope functionCacheMiss(*lookup_cache_miss);
#endif
   // auto key_int = utils::u8_to<KEY>(key, key_length);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafUsingSegment(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key, key_length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
            }
            ensure(sanity_check_result == 0);
         }
// -------------------------------------------------------------------------------------
// Mynote: Guess find key in leaf node
#ifdef LATENCY_BREAKDOWN
         auto leaf_search_timer = timer_registry.registerObject("leaf node search", "leaf_search_lookup");
         leaf_search_timer->start();
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto leaf_search_cache_miss = cache_registry.registerObject(height + 1, "leaf_search_lookup");
         leaf_search_cache_miss->start();
#endif
         s16 pos = leaf->lowerBound<true>(key, key_length);
#ifdef INSTRUMENT_CACHE_MISS
         leaf_search_cache_miss->stop();
#endif
#ifdef LATENCY_BREAKDOWN
         leaf_search_timer->stop();
#endif
         if (pos != -1) {
#ifdef LATENCY_BREAKDOWN
            auto payload_copy_timer = timer_registry.registerObject("payload copy", "payload_copy_lookup");
            Scope payload_timer_scoped(*payload_copy_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
            auto payload_cache_miss = cache_registry.registerObject(height + 2, "payload_copy_lookup");
            Scope payload_cache_scoped(*payload_cache_miss);
#endif
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
            // raise(SIGTRAP); // Do not raise SIGTRAP currently
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}

OP_RESULT BTreeLL::fast_trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback)
{
#ifdef INSTRUMENT_CODE
#ifdef USE_TS
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnstore func: fast_trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<std::string>("learnstore func: fast_trained_lookup purpose: benchmark");
#endif
#endif
   auto lookup_timer = timer_registry.registerObject("lookup", "lookup");
   Scope functionTimer(*lookup_timer);
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnstore func: fast_trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: fast_trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<std::string>("learnstore_func: fast_trained_lookup purpose: benchmark");
#endif
#endif
#endif
#ifdef INSTRUMENT_CACHE_MISS
   static auto cache_registry = AvgCacheMissCounterRegistry("learnstore func: fast_trained_lookup purpose: benchmark");
   auto lookup_cache_miss = cache_registry.registerObject(0, "total_lookup");
   Scope functionCacheMiss(*lookup_cache_miss);
#endif
   volatile u32 mask = 1;
   auto key_length = sizeof(KEY);
   u8 key_bytes[key_length];
   fold(key_bytes, key);
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
#ifdef ATTACH_AT_ROOT
         // fastTrainFindLeafUsingSegmentAttachedAtRoot(leaf, key, key_bytes);
         fastTrainFindLeafUsingSegment(leaf, key, key_bytes);

#else
         fastTrainFindLeafUsingSegment(leaf, key, key_bytes);
#endif
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key_bytes, key_length);
               auto first_key_int = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
            }
            ensure(sanity_check_result == 0);
         }
// -------------------------------------------------------------------------------------
// Mynote: Guess find key in leaf node
#ifdef LATENCY_BREAKDOWN
         auto leaf_search_timer = timer_registry.registerObject("leaf_node_search", "leaf_search_lookup");
         leaf_search_timer->start();
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto leaf_search_cache_miss = cache_registry.registerObject(height + 1, "leaf_search_lookup");
         leaf_search_cache_miss->start();
#endif
         s16 pos = leaf->lowerBound<true>(key_bytes, key_length);
#ifdef INSTRUMENT_CACHE_MISS
         leaf_search_cache_miss->stop();
#endif
#ifdef LATENCY_BREAKDOWN
         leaf_search_timer->stop();
#endif
         if (pos != -1) {
#ifdef LATENCY_BREAKDOWN
            auto payload_copy_timer = timer_registry.registerObject("leaf_node_payload_copy", "payload_copy_lookup");
            Scope payload_timer_scoped(*payload_copy_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
            auto payload_cache_miss = cache_registry.registerObject(height + 2, "payload_copy_lookup");
            Scope payload_cache_scoped(*payload_cache_miss);
#endif
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            // leaf.recheck();
            // raise(SIGTRAP); // Do not raise SIGTRAP currently
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key_bytes, key_length);
               auto first_key_int = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
               // jumpmu_return lookup(key_bytes, key_length, payload_callback);
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}

OP_RESULT BTreeLL::fast_trained_lookup_new(const KEY key)
{
   if (std::shared_lock<std::shared_mutex> lock(model_lock);
       lock.owns_lock() && trained && mapping_key[0] <= key && key <= mapping_key[mapping_key.size() - 1]) {
      auto spline_idx = spline_predictor.GetSplineSegment(key);
      auto leaf_idx = spline_predictor.GetEstimatedPosition(key, spline_idx, mapping_key);
      BufferFrame* leaf_bf = mapping_bfs[leaf_idx];
#ifdef PID_CHECK
      if (auto pid = mapping_pid[leaf_idx]; leaf_bf == nullptr || leaf_bf->header.pid != pid) {
         auto info = BMC::global_bf->getPageinBufferPool(pid);
         leaf_bf = info.bf;
         mapping_bfs[leaf_idx] = leaf_bf;
      }
#endif
      // BufferFrame* leaf_bf = fastTrainFindLeafUsingSegmentAttachedAtRoot(key);
      if (leaf_bf != nullptr) {
         HybridPageGuard<BTreeNode> leaf(leaf_bf);
         s16 pos = 0;
#ifdef MODEL_IN_LEAF_NODE
#ifdef MODEL_LR

         auto swip = leaf.swip();
         auto bf = swip.bfPtr();
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         // if (auto& model = bf->header.model; model.version == bf->page.GSN) {
         if (auto& model = bf->header.model; model.m == 0) {
            auto predict = model.predict(key);

#ifdef EXPONENTIAL_SEARCH
            pos = leaf->exponentialSearch(key_bytes, key_length, predict);
#else
            auto search_bound = bf->header.model.get_searchbound(predict, leaf->count);
            pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
#endif
            // ensure(predict == pos);
            // if (predict != pos) {
            //    std::cout << "leaf node prediction error" << std::endl;
            // }
            //          auto leaf_model = leaf_node_models.find(leaf_bf->header.pid);
            //          if (leaf_model != leaf_node_models.end()) {
            //             // INFO("Found model in leaf node");
            //             auto predict = leaf_model->second.search(key);
            // // INFO("Predicted position %d", predict);
            // #ifdef EXPONENTIAL_SEARCH
            //             pos = leaf->exponentialSearch(key_bytes, key_length, predict);
            // #else
            //             auto search_bound = leaf_model->second.get_searchbound(predict, leaf->count);
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
            //             pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
            // #endif
            //             // INFO("Found position %d", pos);
            //          } else {
            //             std::cout << "All leaf node should have models attached to it" << std::endl;
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             pos = leaf->lowerBound<true>(key_bytes, key_length);
            //          }
         } else {
            pos = leaf->lowerBound<true>(key_bytes, key_length);
         }
#else
         auto spline_idx = leaf_bf->header.splines.GetSplineSegment(key);
         auto predict = leaf_bf->header.splines.GetEstimatedPosition(key, spline_idx);
         auto search_bound = leaf_bf->header.splines.GetSearchBound(predict);
         pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         // auto leaf_model = leaf_node_segments.find(leaf_bf->header.pid);
         // if (leaf_model != leaf_node_segments.end()) {
         //    // INFO("Found model in leaf node");
         //    auto spline_idx = leaf_model->second.GetSplineSegment(key);
         //    // INFO("Spline index %d", spline_idx);
         //    auto predict = leaf_model->second.GetEstimatedPosition(key, spline_idx);
         //    // INFO("Predicted position %d", predict);
         //    auto search_bound = leaf_model->second.GetSearchBound(predict);
         //    // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
         //    pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         //    // INFO("Found position %d", pos);
         // } else {
         //    pos = leaf->lowerBound<true>(key_bytes, key_length);
         // }
#endif
#else
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         pos = leaf->lowerBound<true>(key_bytes, key_length);
#endif
         if (pos != -1) {
            // payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            // INFO("4 leaf_bf: %p key: %lu", leaf_bf, key);
            return OP_RESULT::OK;
         } else {
            // INFO("5 leaf_bf: %p key: %lu", leaf_bf, key);
            // auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
            // auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
            // if (lf_key >= key && uf_key < key) {
            //    return OP_RESULT::NOT_FOUND;
            // }
            auto key_length = sizeof(KEY);
            u8 key_bytes[key_length];
            fold(key_bytes, key);
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            if (sanity_check_result == 0) {
               return OP_RESULT::NOT_FOUND;
            } else {
#ifdef SMO_STATS
               incorrect_leaf++;
               train_signal.notify_one();
#endif
               // auto key_int = utils::u8_to<KEY>(key_bytes, key_length);
               // auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               // auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               // std::cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
               //           << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
               // std::cout << "wrong leaf node" << std::endl;
               // return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   auto key_length = sizeof(KEY);
   u8 key_bytes[key_length];
   fold(key_bytes, key);
   // return lookup(key_bytes, key_length, payload_callback);
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT BTreeLL::fast_trained_lookup_new(const KEY key, function<void(const u8*, u16)> payload_callback)
{
   if (std::shared_lock<std::shared_mutex> lock(model_lock);
       lock.owns_lock() && trained && mapping_key[0] <= key && key <= mapping_key[mapping_key.size() - 1]) {
      // std::cout << "Using segment" << std::endl;
      auto spline_idx = spline_predictor.GetSplineSegment(key);
      auto leaf_idx = spline_predictor.GetEstimatedPosition(key, spline_idx, mapping_key);
      BufferFrame* leaf_bf = mapping_bfs[leaf_idx];
#ifdef PID_CHECK
      if (auto pid = mapping_pid[leaf_idx]; leaf_bf == nullptr || leaf_bf->header.pid != pid) {
         auto info = BMC::global_bf->pageInBufferFrame(pid);
         if (info.bf == nullptr) {
            if (info.lower_fence <= key && key < info.upper_fence) {
               info = BMC::global_bf->getPageinBufferPool(pid);
            } else {
               // misprediction detected
#ifdef SMO_STATS
               incorrect_leaf++;
               train_signal.notify_one();
#endif
               auto key_length = sizeof(KEY);
               u8 key_bytes[key_length];
               fold(key_bytes, key);
               return lookup(key_bytes, key_length, payload_callback);
            }
         }
         // auto info = BMC::global_bf->getPageinBufferPool(pid);
         leaf_bf = info.bf;
         mapping_bfs[leaf_idx] = leaf_bf;
      }
#endif
      // BufferFrame* leaf_bf = fastTrainFindLeafUsingSegmentAttachedAtRoot(key);
      if (leaf_bf != nullptr) {
         HybridPageGuard<BTreeNode> leaf(leaf_bf);
         s16 pos = 0;
#ifdef MODEL_IN_LEAF_NODE
#ifdef MODEL_LR
         auto swip = leaf.swip();
         auto bf = swip.bfPtr();
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         if (auto& model = bf->header.model; model.m == 0) {
            auto predict = bf->header.model.predict(key);
#ifdef EXPONENTIAL_SEARCH
            pos = leaf->exponentialSearch(key_bytes, key_length, predict);
#else
            auto search_bound = bf->header.model.get_searchbound(predict, leaf->count);
            pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
#endif
            //          auto leaf_model = leaf_node_models.find(leaf_bf->header.pid);
            //          if (leaf_model != leaf_node_models.end()) {
            //             // INFO("Found model in leaf node");
            //             auto predict = leaf_model->second.predict(key);
            // // INFO("Predicted position %d", predict);
            // #ifdef EXPONENTIAL_SEARCH
            //             pos = leaf->exponentialSearch(key_bytes, key_length, predict);
            // #else
            //             auto search_bound = leaf_model->second.get_searchbound(predict, leaf->count);
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
            //             pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
            // #endif
            //             // INFO("Found position %d", pos);
            //          } else {
            //             std::cout << "All leaf node should have models attached to it" << std::endl;
            //             auto key_length = sizeof(KEY);
            //             u8 key_bytes[key_length];
            //             fold(key_bytes, key);
            //             pos = leaf->lowerBound<true>(key_bytes, key_length);
            //          }
         } else {
            pos = leaf->lowerBound<true>(key_bytes, key_length);
         }
#else
         auto spline_idx = leaf_bf->header.splines.GetSplineSegment(key);
         auto predict = leaf_bf->header.splines.GetEstimatedPosition(key, spline_idx);
         auto search_bound = leaf_bf->header.splines.GetSearchBound(predict);
         pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         // auto leaf_model = leaf_node_segments.find(leaf_bf->header.pid);
         // if (leaf_model != leaf_node_segments.end()) {
         //    // INFO("Found model in leaf node");
         //    auto spline_idx = leaf_model->second.GetSplineSegment(key);
         //    // INFO("Spline index %d", spline_idx);
         //    auto predict = leaf_model->second.GetEstimatedPosition(key, spline_idx);
         //    // INFO("Predicted position %d", predict);
         //    auto search_bound = leaf_model->second.GetSearchBound(predict);
         //    // INFO("Search bound %d %d", search_bound.begin, search_bound.end);
         //    pos = leaf->binarySearch(key_bytes, key_length, search_bound.begin, search_bound.end);
         //    // INFO("Found position %d", pos);
         // } else {
         //    pos = leaf->lowerBound<true>(key_bytes, key_length);
         // }
#endif
#else
         auto key_length = sizeof(KEY);
         u8 key_bytes[key_length];
         fold(key_bytes, key);
         pos = leaf->lowerBound<true>(key_bytes, key_length);
#endif
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            // INFO("4 leaf_bf: %p key: %lu", leaf_bf, key);
            return OP_RESULT::OK;
         } else {
            // INFO("5 leaf_bf: %p key: %lu", leaf_bf, key);
            // auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
            // auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
            // if (lf_key >= key && uf_key < key) {
            //    return OP_RESULT::NOT_FOUND;
            // }
            auto key_length = sizeof(KEY);
            u8 key_bytes[key_length];
            fold(key_bytes, key);
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            if (sanity_check_result == 0) {
               return OP_RESULT::NOT_FOUND;
            } else {
#ifdef SMO_STATS
               incorrect_leaf++;
               train_signal.notify_one();
#endif
               // auto key_int = utils::u8_to<KEY>(key_bytes, key_length);
               // auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               // auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               // std::cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
               //           << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
               // std::cout << "wrong leaf node" << std::endl;
               // return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   auto key_length = sizeof(KEY);
   u8 key_bytes[key_length];
   fold(key_bytes, key);
   return lookup(key_bytes, key_length, payload_callback);
   return OP_RESULT::NOT_FOUND;
}
OP_RESULT BTreeLL::trained_lookup(KEY key, function<void(const u8*, u16)> payload_callback)
{
#ifdef INSTRUMENT_CODE
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<size_t>("learnstore func: trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<size_t>("learnstore func: trained_lookup purpose: benchmark");
#endif
#endif
   auto lookup_timer = timer_registry.registerObject(0, "lookup");
   Scope functionTimer(*lookup_timer);
#endif
#ifdef LATENCY_BREAKDOWN
#ifdef USE_TSC
#ifdef SIMPLE_SIZE
   static auto timer_registry = SimpleAvgTSCTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTSCTimerRegistry<std::string>("learnstore func: trained_lookup purpose: benchmark");
#endif
#else
#ifdef SIMPLE_SIZE
   static auto timer_registry = AvgTimerRegistry<SIMPLE_SIZE>("learnstore func: trained_lookup purpose: benchmark");
#else
   static auto timer_registry = AvgTimerRegistry<std::string>("learnstore func: trained_lookup purpose: benchmark");
#endif
#endif
   auto leaf_search_timer = timer_registry.registerObject("leaf search", "leaf_search_lookup");
   auto payload_copy_timer = timer_registry.registerObject("payload copy", "payload_copy_lookup");
#endif
#ifdef INSTRUMENT_CACHE_MISS
   static auto cache_registry = AvgCacheMissCounterRegistry("learnstore func: trained_lookup purpose: benchmark");
   auto lookup_cache_miss = cache_registry.registerObject(0, "total_lookup");
   Scope functionCacheMiss(*lookup_cache_miss);
#endif
   // auto key_int = utils::u8_to<KEY>(key, key_length);
   auto key_length = sizeof(KEY);
   u8 key_bytes[key_length];
   fold(key_bytes, key);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         // Mynote: Goes down the tree to the leafnode
         // std::cout << "Running find leaf using segment" << std::endl;
         // BufferFrame newbf;
         // BufferFrame* bfptr = &newbf;
         // findLeafUsingSegment(leaf, key, key_length, bfptr);
         findLeafUsingSegment(leaf, key, key_bytes);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key_bytes, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               auto key_int = utils::u8_to<KEY>(key_bytes, key_length);
               auto first_key_int = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               cout << "sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << key_int << " is_leaf: " << leaf->is_leaf
                    << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
            }
            ensure(sanity_check_result == 0);
         }
// -------------------------------------------------------------------------------------
// Mynote: Guess find key in leaf node
#ifdef LATENCY_BREAKDOWN
         auto leaf_search_timer = timer_registry.registerObject("leaf search lookup", "leaf_search_lookup");
         leaf_search_timer->start();
#endif
#ifdef INSTRUMENT_CACHE_MISS
         auto leaf_search_cache_miss = cache_registry.registerObject(height + 1, "leaf_search_lookup");
         leaf_search_cache_miss->start();
#endif
         s16 pos = leaf->lowerBound<true>(key_bytes, key_length);
#ifdef INSTRUMENT_CACHE_MISS
         leaf_search_cache_miss->stop();
#endif
#ifdef LATENCY_BREAKDOWN
         leaf_search_timer->stop();
#endif
         if (pos != -1) {
#ifdef LATENCY_BREAKDOWN
            auto payload_copy_timer = timer_registry.registerObject("payload copy timer", "payload_copy_lookup");
            Scope payload_timer_scoped(*payload_copy_timer);
#endif
#ifdef INSTRUMENT_CACHE_MISS
            auto payload_cache_miss = cache_registry.registerObject(height + 2, "payload_copy_lookup");
            Scope payload_cache_scoped(*payload_cache_miss);
#endif
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
            // raise(SIGTRAP); // Do not raise SIGTRAP currently
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
//----------------------------------------------------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAscAllSeg(std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback)
{
   // MyNote:: Slice -> std::string_view
   jumpmuTry()
   {  // TODO:: looke more into described interator
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto count = 0;
      while (true) {
         // std::cout << " count: " << count << std::endl;
         // INFO("count: %d", count);
         count++;
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         }
         if (iterator.nextwithseg() != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }  // namespace btree
   }  // namespace btree
   jumpmuCatch()
   {
      ensure(false);
   }
}  // namespace storage
//----------------------------------------------------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAscAll(std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback)
{
   // MyNote:: Slice -> std::string_view
   jumpmuTry()
   {  // TODO:: looke more into described interator
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         // if (!callback(key.data(), key.length(), value.data(), value.length())) {
         //    jumpmu_return OP_RESULT::OK;
         // } else {
         //    if (iterator.next() != OP_RESULT::OK) {
         //       jumpmu_return OP_RESULT::NOT_FOUND;
         //    }
         // }
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         }
         if (iterator.next() != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }  // namespace btree
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>,
                           u64 rlength)
{
   // MyNote:: Slice -> std::string_view
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      // TODO:: looke more into described interator
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto count = 0;
      while (rlength == 0 || count < rlength) {
         count++;
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         }
         if (iterator.next() != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.prev() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
OP_RESULT BTreeLL::scanAscSeg(KEY key,
                              u8* start_key,
                              u16 key_length,
                              std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                              function<void()>,
                              u64 rlength)
{
   Slice key1(start_key, key_length);
   jumpmuTry()
   {
      // TODO:: looke more into described interator
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekwithseg(key1);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto count = 0;
      while (rlength == 0 || count < rlength) {
         count++;
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         }
         if (iterator.nextwithseg() != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDescSeg(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.prev() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
OP_RESULT BTreeLL::fast_insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   Slice value(o_value, o_value_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.insertKVFast(key, value);
      ensure(ret == OP_RESULT::OK);
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(key.length() + value.length());
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = key.length();
         wal_entry->value_length = value.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + key.length(), value.data(), value.length());
         wal_entry.submit();
      } else {
         iterator.leaf.incrementGSN();
         // train_leaf_signal.notify_one();
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
OP_RESULT BTreeLL::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   Slice value(o_value, o_value_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.insertKV(key, value);
      ensure(ret == OP_RESULT::OK);
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(key.length() + value.length());
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = key.length();
         wal_entry->value_length = value.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + key.length(), value.data(), value.length());
         wal_entry.submit();
      } else {
         iterator.leaf.incrementGSN();
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSize(u8* o_key,
                                  u16 o_key_length,
                                  function<void(u8* payload, u16 payload_size)> callback,
                                  WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto current_value = iterator.mutableValue();
      if (FLAGS_wal) {
         // if it is a secondary index, then we can not use updateSameSize
         assert(wal_update_generator.entry_size > 0);
         // -------------------------------------------------------------------------------------
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key.length() + wal_update_generator.entry_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = key.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         wal_update_generator.before(current_value.data(), wal_entry->payload + key.length());
         // The actual update by the client
         callback(current_value.data(), current_value.length());
         wal_update_generator.after(current_value.data(), wal_entry->payload + key.length());
         wal_entry.submit();
      } else {
         callback(current_value.data(), current_value.length());
         iterator.leaf.incrementGSN();
      }
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* o_key, u16 o_key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      Slice value = iterator.value();
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length);
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = o_key_length;
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + o_key_length, value.data(), value.length());
         wal_entry.submit();
      } else {
         iterator.leaf.incrementGSN();
      }
      ret = iterator.removeCurrent();
      ensure(ret == OP_RESULT::OK);
      iterator.mergeIfNeeded();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch()
   {
      ensure(false);
   }
}
void BTreeLL::scanAll()
{
   auto leaf_count = 0ul;
   auto total_kv = 0ul;
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      DEBUG_BLOCK()
      {
         std::cout << "seeking the minimum leaf" << std::endl;
      }
      if (ret != OP_RESULT::OK) {
         std::cout << "seekFirstLeaf: " << static_cast<u8>(ret) << std::endl;
         return;
      }
      do {
         auto key = iterator.key();
         auto value = iterator.value();
         auto key_itr_int = utils::string_view_to<KEY>(key);
         ;
         u8 keys[255];
         iterator.leaf->copyFullKey(0, keys);
         auto key_len = iterator.leaf->getFullKeyLen(0);
         auto key_int = utils::u8_to<KEY>(keys, key_len);
         // ensure(key_itr_int == key_int);
         auto swip = iterator.leaf.swip();
         auto upper_fence_key_len = iterator.leaf->lower_fence.length;
         auto lower_fence_key_len = iterator.leaf->upper_fence.length;
         auto lower_fence_key = iterator.leaf->getLowerFenceKey();
         auto upper_fence_key = iterator.leaf->getUpperFenceKey();
         auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
         auto bf = swip.bfPtr();
         auto bfpid = bf->header.pid;
         auto leaf_fanout = iterator.leaf->count;
         iterator.leaf->copyFullKey(leaf_fanout - 1, keys);
         auto last_key_len = iterator.leaf->getFullKeyLen(leaf_fanout - 1);
         auto last_key_int = utils::u8_to<KEY>(keys, last_key_len);

         total_kv += leaf_fanout;
         DEBUG_BLOCK()
         {
            std::cout << " leaf_count: " << leaf_count << " first_key: " << key_int << " last_key: " << last_key_int << " pid : " << bfpid
                      << " lower_fence_key : " << lower_fence_key_int << " upper_fence_key: " << upper_fence_key_int
                      << " leaf_fanout: " << leaf_fanout << std::endl;
            // ensure(key_int == lower_fence_key_int);
         }
         leaf_count++;
      } while (iterator.nextLeaf());
   }
   std::cout << "leaf count: " << leaf_count << std::endl;
   std::cout << "total kv:" << total_kv << std::endl;
}

void BTreeLL::slot_keys(std::vector<KEY>& keys, std::vector<PID>& pids, std::vector<BufferFrame*>& bfs)
{
   u32 volatile mask = 1;
   auto target_guard = HybridPageGuard<BTreeNode>();
   // INFO("starting slot_keys");
   pids.clear();
   keys.clear();
   bfs.clear();
   while (true) {
      std::vector<KEY> node_key;
      std::vector<PID> node_pid;
      std::vector<BufferFrame*> node_bf;
      jumpmuTry()
      {
         findFirstLeafParentCanJump(target_guard);
         // INFO("findFirstLeafParentCanJump done leaf count: %lu", target_guard->count);
         // pids.clear();
         // keys.clear();
         // bfs.clear();
         auto count = target_guard->count;
         node_key.reserve(count);
         node_pid.reserve(count);
         node_bf.reserve(count);
         for (auto i = 0; i < target_guard->count; i++) {
            target_guard.recheck();
            auto key_len = target_guard->getFullKeyLen(i);
            u8 key_bytes[sizeof(KEY)];
            target_guard->copyFullKey(i, key_bytes);
            auto int_key = utils::u8_to<KEY>(key_bytes, sizeof(KEY));
            auto c_swip = target_guard->getChild(i);
            BufferFrame* bfptr = nullptr;
            PID child_pid = 0;
            if (c_swip.isHOT()) {
               bfptr = c_swip.bfPtr();
               child_pid = bfptr->header.pid;
            } else if (c_swip.isCOOL()) {
               bfptr = c_swip.bfPtrAsHot();
               child_pid = bfptr->header.pid;
            } else {
               child_pid = c_swip.asPageID();
            }
            node_pid.push_back(child_pid);
            node_key.push_back(int_key);
            node_bf.push_back(bfptr);
            // INFO("Find first parent count: %lu", i);
            DEBUG_BLOCK()
            {
               // Check if the key is present in the child_pid
               // should be present as the maximum key
               auto leaf = HybridPageGuard(target_guard, c_swip);
               auto leaf_last_key_len = leaf->getFullKeyLen(leaf->count - 1);
               // u8 leaf_last_key_bytes[leaf_last_key_len];
               u8 leaf_last_key_bytes[sizeof(KEY)];
               for (auto i = 0; i < sizeof(KEY); i++) {
                  leaf_last_key_bytes[i] = 0;
               }
               leaf->copyFullKey(leaf->count - 1, leaf_last_key_bytes);
               // auto leaf_last_key = utils::u8_to<KEY>(leaf_last_key_bytes, leaf_last_key_len);
               auto leaf_last_key = utils::u8_to<KEY>(leaf_last_key_bytes, sizeof(KEY));
               ensure(leaf_last_key <= int_key);
               if (leaf_last_key != int_key) {
                  cout << "[last_keys 0] i: " << i << " inner_count: " << target_guard->count << " slot key: " << int_key
                       << " slot key len: " << key_len << " child pid: " << child_pid << " leaf_count: " << leaf->count
                       << " is_leaf: " << leaf->is_leaf << " last key: " << leaf_last_key << " last key len:" << leaf_last_key_len << endl;
               };
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               // auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), sizeof(KEY));
               // ensure(uf_key >= int_key);
               auto c_swip = target_guard->getChild(i + 1);
               leaf = HybridPageGuard(target_guard, c_swip);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               ensure(lf_key >= int_key);
               ensure(uf_key <= int_key);
               if (!(int_key <= lf_key && int_key >= uf_key))
                  cout << "[slot_keys 0] sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << int_key << " is_leaf: " << leaf->is_leaf
                       << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
               ensure(uf_key == int_key);
               // std::cout << i << " " << int_key << std::endl;
            }
         }
         // INFO("start keys first leaf inner completed");

         // auto lower_fence_key = target_guard->getLowerFenceKey();
         // auto lower_fence_key_len = target_guard->lower_fence.length;
         target_guard.recheck();
         auto upper_fence_key = target_guard->getUpperFenceKey();
         auto upper_fence_key_len = target_guard->upper_fence.length;
         auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         auto c_swip = target_guard->upper;
         BufferFrame* bfptr = nullptr;
         PID child_pid = 0;
         if (c_swip.isHOT()) {
            bfptr = c_swip.bfPtr();
            child_pid = bfptr->header.pid;
         } else if (c_swip.isCOOL()) {
            bfptr = c_swip.bfPtrAsHot();
            child_pid = bfptr->header.pid;
         } else {
            child_pid = c_swip.asPageID();
         }
         node_pid.push_back(child_pid);
         node_bf.push_back(bfptr);
         if (upper_fence_key_len != 0)
            node_key.push_back(upper_fence_key_int);
         DEBUG_BLOCK()
         {
            // Check if the key is present in the child_pid
            // should be present as the maximum key
            if (upper_fence_key != nullptr) {
               auto leaf = HybridPageGuard(target_guard, c_swip);
               auto leaf_last_key_len = leaf->getFullKeyLen(leaf->count - 1);
               u8 leaf_last_key_bytes[leaf_last_key_len];
               leaf->copyFullKey(leaf->count - 1, leaf_last_key_bytes);
               auto leaf_last_key = utils::u8_to<KEY>(leaf_last_key_bytes, leaf_last_key_len);
               // keys.push_back(leaf_last_key);
               // ensure(leaf_last_key == upper_fence_key_int);
               if (leaf_last_key != upper_fence_key_int) {
                  cout << "[last_keys 1] inner_count: " << target_guard->count << " upper_fence_key: " << upper_fence_key_int
                       << " upper_fence_key_len: " << upper_fence_key_len << " last key: " << leaf_last_key << " last key len: " << leaf_last_key_len
                       << endl;
               };
               auto uf_key = utils::u8_to<KEY>(leaf->getUpperFenceKey(), leaf->upper_fence.length);
               auto c_swip = target_guard->getChild(target_guard->count);
               auto lf_key = utils::u8_to<KEY>(leaf->getLowerFenceKey(), leaf->lower_fence.length);
               if (upper_fence_key_int <= lf_key || upper_fence_key_int > uf_key)
                  cout << "[slot_keys 1] sanity_check_result: 0 leaf_count: " << leaf->count << " key: " << upper_fence_key_int
                       << " is_leaf: " << leaf->is_leaf << " lower_fence: " << lf_key << " upper_fence: " << uf_key << endl;
               ensure(uf_key == upper_fence_key_int);
            }
         }
         // auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
         for (auto i = 0; i < node_key.size(); i++) {
            keys.push_back(node_key[i]);
            pids.push_back(node_pid[i]);
            bfs.push_back(node_bf[i]);
         }
         for (auto j = node_key.size(); j < node_pid.size(); j++) {
            pids.push_back(node_pid[j]);
            bfs.push_back(node_bf[j]);
         }
         // INFO("start keys first leaf parent fence completed");
         jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES();
         continue;
      }
   }
   // INFO("start keys first leaf completed");
   u16 key_length = target_guard->upper_fence.length + 1;
   u8 key[key_length];
   std::memcpy(key, target_guard->getUpperFenceKey(), target_guard->upper_fence.length);
   key[key_length - 1] = 0;
   while (target_guard->getUpperFenceKey() != nullptr) {
      std::vector<KEY> node_key;
      std::vector<PID> node_pid;
      std::vector<BufferFrame*> node_bf;
      jumpmuTry()
      {
         // auto lower_fence_key = target_guard->getLowerFenceKey();
         // auto lower_fence_key_len = target_guard->lower_fence.length;
         // auto upper_fence_key = target_guard->getUpperFenceKey();
         // auto upper_fence_key_len = target_guard->upper_fence.length;
         // auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         // auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;

         findLeafParentCanJump(target_guard, key, key_length);
         // TODO: arrary or vector of key of length target_guard->count
         // TODO: push temporarily to that array
         auto count = target_guard->count;
         node_key.clear();
         node_pid.clear();
         node_bf.clear();
         node_key.reserve(count);
         node_pid.reserve(count);
         node_bf.reserve(count);
         for (auto i = 0; i < target_guard->count; i++) {
            target_guard.recheck();
            auto key_len = target_guard->getFullKeyLen(i);
            u8 key_bytes[sizeof(KEY)];
            target_guard->copyFullKey(i, key_bytes);
            auto key_int = utils::u8_to<KEY>(key_bytes, sizeof(KEY));
            auto c_swip = target_guard->getChild(i);
            BufferFrame* bfptr = nullptr;
            PID child_pid = 0;
            if (c_swip.isHOT()) {
               bfptr = c_swip.bfPtr();
               child_pid = bfptr->header.pid;
            } else if (c_swip.isCOOL()) {
               bfptr = c_swip.bfPtrAsHot();
               child_pid = bfptr->header.pid;
            } else {
               child_pid = c_swip.asPageID();
            }
            node_pid.push_back(child_pid);
            node_bf.push_back(bfptr);
            node_key.push_back(key_int);
         }
         // auto lower_fence_key = target_guard->getLowerFenceKey();
         // auto lower_fence_key_len = target_guard->lower_fence.length;
         // INFO("leaf count: %lu", keys.size());
         auto upper_fence_key = target_guard->getUpperFenceKey();
         auto upper_fence_key_len = target_guard->upper_fence.length;
         key_length = upper_fence_key_len + 1;
         std::memcpy(key, target_guard->getUpperFenceKey(), target_guard->upper_fence.length);
         key[key_length - 1] = 0;
         auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         // auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
         auto c_swip = target_guard->upper;
         BufferFrame* bfptr = nullptr;
         PID child_pid = 0;
         if (c_swip.isHOT()) {
            bfptr = c_swip.bfPtr();
            child_pid = bfptr->header.pid;
         } else if (c_swip.isCOOL()) {
            bfptr = c_swip.bfPtrAsHot();
            child_pid = bfptr->header.pid;
         } else {
            child_pid = c_swip.asPageID();
         }
         node_pid.push_back(child_pid);
         node_bf.push_back(bfptr);
         if (upper_fence_key_len != 0)
            node_key.push_back(upper_fence_key_int);
         // TODO:: push to the global array
         // INFO("upper fence added leaf count: %lu", keys.size());
         for (auto i = 0; i < node_key.size(); i++) {
            keys.push_back(node_key[i]);
            pids.push_back(node_pid[i]);
            bfs.push_back(node_bf[i]);
         }
         for (auto j = node_key.size(); j < node_pid.size(); j++) {
            pids.push_back(node_pid[j]);
            bfs.push_back(node_bf[j]);
         }
         jumpmu_continue;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES();
      }
   }
   // INFO("slot_keys completed");
}
void BTreeLL::forced_train(const int max_error)
{
   fast_train(max_error);
#ifdef MODEL_IN_LEAF_NODE
   train_leaf_nodes_bf(1);
#endif
}
void BTreeLL::fast_train(const int max_error)
{
   std::vector<KEY> keys;
   std::vector<PID> pids;
   std::vector<BufferFrame*> bfs;
   slot_keys(keys, pids, bfs);
   std::unique_lock<std::shared_mutex> lock(model_lock);
   INFO("Training started");
   DEBUG_BLOCK()
   {
      std::cout << "Found leafs: " << keys.size() << " pid size: " << pids.size() << std::endl;
   }
   volatile u32 mask = 1;
   max_error_ = max_error;
   trained = false;
#ifdef COMPACT_MAPPING
   mapping_key.clear();
   mapping_pid.clear();
   mapping_bfs.clear();
#else
   secondary_mapping_pid.clear();
   secondary_mapping_bf.clear();
#endif
#ifdef SMO_STATS
   num_splits = 0;
   incorrect_leaf = 0;
#endif
   // Note:: we need to clear in the buffer pool header iformation as well
   attached_segments.clear();
   DEBUG_BLOCK()
   {
      std::cout << "Height: " << getHeight() << std::endl;
      std::cout << "Max Error: " << max_error << std::endl;
   }
   // INFO("Start training height: %lu max_error: %lu", getHeight(), max_error);
   auto leaf_count = 0ul;
   /* Segments if we want to add the first key. Should not be there.*/
   /**
   auto key = std::numeric_limits<KEY>::min();

   sbd.AddKey(key);
#ifdef COMPACT_MAPPING
   mapping_key.push_back(key);
   mapping_pid.push_back(pids[0]);
   mapping_bfs.push_back(bfs[0]);
#else
   secondary_mapping_pid.push_back(make_pair(key, pids[0]));
   secondary_mapping_bf.push_back(make_pair(key, bfs[0]));
#endif
   */
   for (auto i = 0; i < keys.size(); i++) {
      auto key = keys[i];
      auto bfpid = pids[i];
      auto bf = bfs[i];
      // INFO("Key: %lu index: %lu pid: %lu bf: %lu", key, i, bfpid, bf);
      // sbd.AddKey(key);
      // rsb.AddKey(key);
#ifdef COMPACT_MAPPING
      mapping_key.push_back(key);
      mapping_pid.push_back(bfpid);
      mapping_bfs.push_back(bf);
#else
      secondary_mapping_pid.push_back(make_pair(key, bfpid));
      secondary_mapping_bf.push_back(make_pair(key, bf));
#endif
   }
   // Attach last key and leaf node
   // auto key = std::numeric_limits<KEY>::max();
   auto bfpid = pids[pids.size() - 1];
   auto bf = bfs[bfs.size() - 1];
#ifdef COMPACT_MAPPING
   // mapping_key.push_back(key);
   mapping_pid.push_back(bfpid);
   mapping_bfs.push_back(bf);
#else
   secondary_mapping_pid.push_back(make_pair(key, bfpid));
   secondary_mapping_bf.push_back(make_pair(key, bf));
#endif
#ifdef MODEL_SEG
   auto sbd = spline::Builder<KEY>(max_error);
   for (auto i = 0; i < keys.size(); i++) {
      auto key = keys[i];
      sbd.AddKey(key);
   }
   auto segments = sbd.Finalize();
   spline_predictor = spline::RadixSpline<KEY>(max_error, pids.size(), segments);
#endif
   // INFO("spline predictor create. segments: %lu", segments.size() - 1);
   DEBUG_BLOCK()
   {
      std::cout << "segments count: " << segments.size() - 1 << std::endl;
   }
   // Print all the segments
   DEBUG_BLOCK()
   {
      for (auto seg : segments) {
         std::cout << " x: " << seg.x << " y: " << seg.y << std::endl;
         INFO("x: %lu y: %lu", seg.x, seg.y);
      }
   }
   // for (auto i = 1ul; i < spline_predictor.spline_points_.size(); i++) {
   //    attach_segment(i);
   // }
   // BMC::global_bf->clearAttachedSegments();
   // for (auto keys : attached_segments) {
   //    // std::cout << "pid: " << keys.first << " segments_size: " << keys.second.size() << " segments: ";
   //    // INFO("pid: %lu segments_size: %lu", keys.first, keys.second.size());
   //    BMC::global_bf->getPageinBufferPool(keys.first)->header.attached_segments = &(BMC::attached_segments[keys.first]);
   // }
#ifdef ATTACH_SEGMENTS_STATS
   for (auto keys : attached_segments) {
      std::cout << "pid: " << keys.first << " attached_segments_size: " << keys.second.size() << " segments: ";
      for (auto i : keys.second) {
         std::cout << i << ", ";
      }
      std::cout << std::endl;
   }
#endif
   DEBUG_BLOCK()
   {
      std::cout << "Finished attaching segments" << std::endl;
      std::cout << "attach_segments_load_factor: " << attached_segments.load_factor() << std::endl;
   }
#ifdef SMO_STATS
   num_splits = 0;
   incorrect_leaf = 0;
#endif
   trained = true;

   INFO("Training End");
   return;
}

void BTreeLL::auto_train(const int max_error)
{
   // if (!bg_training_thread) {
   // return;

   bg_training_thread = true;
   // creates a thread that automatically trains the tree
   max_error_ = (max_error == 0) ? max_error_ : max_error;
   training_thread = std::thread([this]() {
      // auto last_train_time = std::chrono::steady_clock::now();
      const auto WAIT_TIME = 2;
      while (bg_training_thread) {
         {
            std::unique_lock<std::mutex> lk(this->train_signal_lock);
            this->train_signal.wait(lk, [this] {
               const auto START_HEIGHT = 3;
               const auto INCORRECT_TOLERANCE = 1;
               auto first_train = (!trained && this->getHeight() > START_HEIGHT);
               auto incorrect_tolerance = (trained && (static_cast<float>(incorrect_leaf) / num_splits) > INCORRECT_TOLERANCE);
               INFO("Checking at height: %lu incorrect_leaf: %lu num_splits: %lu first_train: %u incorrect_tolearance: %u", this->getHeight(),
                    incorrect_leaf, num_splits, first_train, incorrect_tolerance);
               // auto write_heavy = (static_cast<float>(num_splits) / this->mapping_pid.size() > 0.10);
               // INFO("Checking if it needs to be retrained");
               // return first_train || read_heavy || write_heavy;
               return bg_training_thread ? (first_train || incorrect_tolerance) : true;
            });
         }
         INFO("Training at height: %lu incorrect_leaf: %lu num_splits: %lu", this->getHeight(), incorrect_leaf, num_splits);
         // auto current_time = std::chrono::steady_clock::now();
         // if ((current_time - last_train_time) < std::chrono::seconds(WAIT_TIME)) {
         //    trained = false;
         //    continue;
         // }
         if (bg_training_thread)
            this->fast_train(max_error_);
         // last_train_time = std::chrono::steady_clock::now();
         // WAIT_TIME = (last_train_time - current_time);
         // std::this_thread::sleep_for(std::chrono::seconds(5));
      }
   });
   leaf_training_thread = std::thread([this]() {
      const auto WAIT_TIME = 1;
      while (bg_training_thread) {
         {
            // std::unique_lock<std::mutex> lk(this->train_leaf_signal_lock);
            // this->train_leaf_signal.wait(lk, [this] { return true; });
         }
         if (bg_training_thread) {
            this->train_leaf_nodes_bf(1);
         }
         std::this_thread::sleep_for(std::chrono::seconds(WAIT_TIME));
      }
   });
   training_thread.detach();
   leaf_training_thread.detach();
}

void BTreeLL::train_leaf_nodes_bf(size_t maxerror)
{
   INFO("Starting training of leaf nodes");
   auto trained_leaf_nodes = 0;
   for (int i = 0; i < BMC::global_bf->dram_pool_size; i++) {
      auto bf = BMC::global_bf->bfs + i;
      if (bf->header.state != BufferFrame::STATE::FREE) {
         HybridPageGuard<BTreeNode> node(bf);
         if (auto& model = bf->header.model; node->is_leaf && bf->page.GSN != model.version) {
            if (train_leaf_node(node, maxerror)) {
               trained_leaf_nodes++;
            }
         }
      }
   }
   INFO("Trained leaf nodes: %lu", trained_leaf_nodes);
}

void BTreeLL::train_leaf_nodes(size_t maxerror)
{
   auto leaf_count = 0ul;
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      DEBUG_BLOCK()
      {
         std::cout << "seeking the minimum leaf" << std::endl;
      }
      if (ret != OP_RESULT::OK) {
         std::cout << "seekFirstLeaf: " << static_cast<u8>(ret) << std::endl;
         return;
      }
      do {
         // Train individual leaf nodes
         train_leaf_node(iterator.leaf, maxerror);
         leaf_count++;
      } while (iterator.nextLeaf());
   }
   std::cout << "leaf count: " << leaf_count << std::endl;
}

bool BTreeLL::train_leaf_node(HybridPageGuard<BTreeNode>& leaf, size_t maxerror)
{
   // check if the node is a leaf node
   if (!(leaf->is_leaf)) {
      return false;
   }
   // Get all the keys in the leaf node
   auto size = leaf->count;
   std::vector<KEY> keys;
   keys.reserve(size);
   for (auto i = 0; i < size; i++) {
      const auto key_size = sizeof(KEY);
      u8 key_bytes[key_size];
      leaf->copyFullKey(i, key_bytes);
      auto key_int = utils::u8_to<KEY>(key_bytes, key_size);
      keys.push_back(key_int);
   }
   // Train the leaf node
   auto swip = leaf.swip();
   auto bf = swip.bfPtr();
   auto pid = bf->header.pid;
#ifdef MODEL_LR
   auto linear = learnedindex<KEY>();
   linear.train(keys, bf->page.GSN);
   leaf_node_models[pid] = linear;
   bf->header.model = linear;
#else
   auto sbd = spline::Builder<KEY>(maxerror);
   for (auto i = 0; i < size; i++) {
      sbd.AddKey(keys[i]);
   }
   auto spline = sbd.Finalize();
   auto leaf_predictor = spline::RadixSpline<KEY>(maxerror, size, spline);
   leaf_node_segments[pid] = leaf_predictor;
   bf->header.splines = leaf_node_segments[pid];
#endif
   return true;
}

void BTreeLL::train(const int max_error)
{
   volatile u32 mask = 1;
   max_error_ = max_error_;
#ifdef COMPACT_MAPPING
   mapping_key.clear();
   mapping_pid.clear();
#else
   secondary_mapping_pid.clear();
#endif
   attached_segments.clear();
   DEBUG_BLOCK()
   {
      std::cout << "Running training from BtreeLL" << std::endl;
      // std::cout << "No of pages: " << countPages() << std::endl;
      std::cout << "Height: " << getHeight() << std::endl;
      std::cout << "Max Error: " << max_error << std::endl;
   }
   /*
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      if (ret != OP_RESULT::OK) {
         return ret;
      }
      do {
         auto key = iterator.key();
         auto value = iterator.value();
         auto swip = iterator.leaf.swip();
         auto bf = swip.bfPtr();
         auto bfpid = bf->header.pid;
         auto key_int = string_view_to<uint64_t>(key);
         std::cout << " key: " << key_int << std::endl;
      } while (iterator.next() == OP_RESULT::OK);
   }
   */
   auto leaf_count = 0;
   auto total_kv = 0;
   auto sbd = spline::Builder<KEY>(max_error);
   auto secondary_mapping = std::vector<std::pair<KEY, Swip<BTreeNode> > >();
   // auto secondary_mapping_pid = std::vector<std::pair<KEY, PID>>();
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekFirstLeaf();
      DEBUG_BLOCK()
      {
         std::cout << "seeking the minimum leaf" << std::endl;
      }
      if (ret != OP_RESULT::OK) {
         std::cout << "seekFirstLeaf: " << static_cast<u8>(ret) << std::endl;
         return;
      }
      do {
         auto key = iterator.key();
         auto value = iterator.value();
         auto key_itr_int = utils::string_view_to<KEY>(key);
         ;
         u8 keys[255];
         iterator.leaf->copyFullKey(0, keys);
         auto key_len = iterator.leaf->getFullKeyLen(0);
         auto key_int = utils::u8_to<KEY>(keys, key_len);
         ensure(key_itr_int == key_int);
         auto swip = iterator.leaf.swip();
         auto upper_fence_key_len = iterator.leaf->lower_fence.length;
         auto lower_fence_key_len = iterator.leaf->upper_fence.length;
         auto lower_fence_key = iterator.leaf->getLowerFenceKey();
         auto upper_fence_key = iterator.leaf->getUpperFenceKey();
         auto upper_fence_key_int = (upper_fence_key != nullptr) ? utils::u8_to<KEY>(upper_fence_key, upper_fence_key_len) : 0;
         auto lower_fence_key_int = (lower_fence_key != nullptr) ? utils::u8_to<KEY>(lower_fence_key, lower_fence_key_len) : 0;
         auto bf = swip.bfPtr();
         auto bfpid = bf->header.pid;
         auto leaf_fanout = iterator.leaf->count;
         iterator.leaf->copyFullKey(leaf_fanout - 1, keys);
         auto last_key_len = iterator.leaf->getFullKeyLen(leaf_fanout - 1);
         auto last_key_int = utils::u8_to<KEY>(keys, last_key_len);

         total_kv += leaf_fanout;
         DEBUG_BLOCK()
         {
            std::cout << " leaf_count: " << leaf_count << " first_key: " << key_int << " last_key: " << last_key_int << " pid : " << bfpid
                      << " lower_fence_key : " << lower_fence_key_int << " upper_fence_key: " << upper_fence_key_int
                      << " leaf_fanout: " << leaf_fanout << std::endl;
            // ensure(key_int == lower_fence_key_int);
         }
         sbd.AddKey(key_int);
         for (auto i = 0; i < iterator.leaf->hint_count; i++) {
            std::cout << "pid: " << bfpid << " tag_" << i << ": " << static_cast<uint8_t>(iterator.leaf->tag[i]) << std::endl;
         }
#ifdef COMPACT_MAPPING
         mapping_key.push_back(key_int);
         mapping_pid.push_back(bfpid);
#else
         secondary_mapping_pid.push_back(make_pair(key_int, bfpid));
         secondary_mapping.push_back(make_pair(key_int, swip));
#endif
         leaf_count++;
      } while (iterator.nextLeaf());
   }
   DEBUG_BLOCK()
   {
      std::cout << "leaf count: " << leaf_count << std::endl;
      std::cout << "total kv: " << total_kv << std::endl;
   }
   auto segments = sbd.Finalize();
#ifdef COMPACT_MAPPING
   // ensure(mapping_key.size() == mapping_pid.size());
   // ensure(mapping_key.size() == leaf_count);
#else
   ensure(secondary_mapping_pid.size() == secondary_mapping.size());
   ensure(secondary_mapping_pid.size() == leaf_count);
#endif
   spline_predictor = spline::RadixSpline<KEY>(max_error, leaf_count, segments);
   DEBUG_BLOCK()
   {
      std::cout << "segments count: " << segments.size() << std::endl;
   }
   // Print all the segments
   DEBUG_BLOCK()
   {
      for (auto seg : segments) {
         std::cout << " x: " << seg.x << " y: " << seg.y << std::endl;
      }
   }
   for (auto i = 1; i < spline_predictor.spline_points_.size(); i++) {
      DEBUG_BLOCK()
      {
         std::cout << "Trying to attach segment: " << i << std::endl;
      }
      attach_segment(i);
      DEBUG_BLOCK()
      {
         std::cout << "Finished attaching segment: " << i << std::endl;
      }
   }
   DEBUG_BLOCK()
   {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> target_guard(meta_node_bf);
         HybridPageGuard<BTreeNode> new_target_guard(target_guard, target_guard->upper);
         // examine_spline_idx(new_target_guard, height);
         // jumpmu_break;
         std::cout << " pid: " << new_target_guard.bf->header.pid << " slot key: ";
         for (auto i = 0; i < new_target_guard->count; i++) {
            u8 buffer[255];
            new_target_guard->copyFullKey(i, buffer);
            auto key_len = new_target_guard->getFullKeyLen(i);
            auto key = utils::u8_to<KEY>(buffer, key_len);
            std::cout << key << " ";
         }
         std::cout << std::endl;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES();
      }
   }
#ifdef ATTACH_SEGMENTS_STATS
   for (auto keys : attached_segments) {
      std::cout << "pid: " << keys.first << " attached_segments_size: " << keys.second.size() << " segments: ";
      for (auto i : keys.second) {
         std::cout << i << ", ";
      }
      std::cout << std::endl;
   }
#endif
   DEBUG_BLOCK()
   {
      std::cout << "attach_segments_load_factor: " << attached_segments.load_factor() << std::endl;
   }
   // Test we can access all the Pages using Pid:w

   /*
   DEBUG_BLOCK() {
   for (auto i= 0; i < secondary_mapping.size(); i++ ) {
      u8 failcount = 0;
      while (failcount < 5) {
         jumpmuTry () {
            auto map = secondary_mapping[i];
            auto key = map.first;
            auto swip = map.second;
            auto pid = secondary_mapping_pid[i].second;
            auto loadbf = BMC::global_bf->pageInBufferFrame(pid);
            if (loadbf == nullptr) {
               std::cout << "Loading the bf from disk" << std::endl;
               BufferFrame loadedbf;
               BMC::global_bf->getPage(pid, &loadedbf);
               BTreeNode* loadnode= reinterpret_cast<BTreeNode*>(loadedbf.page.dt);
               auto key_len = loadnode->getFullKeyLen (0);
               u8 char_keys[128];
               loadnode->copyFullKey (0, char_keys);
               auto obt_key = utils::u8_to<KEY> (char_keys, sizeof(KEY));
               std::cout << i << " count: " << loadnode->count << " is_leaf: " << loadnode->is_leaf
                      << " key: " << obt_key << " length: " << key_len
                      << std::endl;
            }
            else {
               BTreeNode* loadnode= reinterpret_cast<BTreeNode*>(loadbf->page.dt);
               auto key_len = loadnode->getFullKeyLen (0);
               u8 char_keys[128];
               loadnode->copyFullKey (0, char_keys);
               auto obt_key = utils::u8_to<KEY> (char_keys, sizeof(KEY));
               std::cout << i << " count: " << loadnode->count << " is_leaf: " << loadnode->is_leaf
                      << " key: " << obt_key << " length: " << key_len
                      << std::endl;
            }
            // HybridPageGuard loadbf_guard = HybridPageGuard<BTreeNode>(&loadbf);
            // std::cout << i << " count: " << loadbf_guard->count << " is_leaf: " << loadbf_guard->is_leaf << std::endl;
            auto newswip = Swip<BTreeNode> ();
            newswip.evict (pid);
            // std::cout << " swip_hot: " << swip.isHOT() << " swip_cool: " << swip.isCOOL() <<
            // std::endl; auto n_guard = HybridPageGuard<BTreeNode>(bf); auto n_guard =
            // HybridPageGuard<BTreeNode>(dt_id);
            auto n_guard = HybridPageGuard<BTreeNode> (meta_node_bf);
            // auto n_gurad_sh = SharedPageGuard(HybridPageGuard<BTreeNode>(meta_node_bf));
            // Lets force the load
            // auto c_guard = HybridPageGuard<BTreeNode>(n_guard,swip);
            auto c_guard = HybridPageGuard<BTreeNode> (n_guard, newswip);
            // auto c_guard = HybridPageGuard<BTreeNode>(n_guard_sh,newswip);
            // assert(c_guard.bf == swip.bfPtr());
            assert (c_guard.bf->header.pid == pid);
            auto key_len = c_guard->getFullKeyLen (0);
            u8 char_keys[128];
            c_guard->copyFullKey (0, char_keys);
            auto obt_key = utils::u8_to<uint64_t> (char_keys, 8);
            // auto obt_key = reinterpret_cast<uint64_t *>(c_guard->getKey(0));
            auto bf_is_hot = c_guard.bf->header.state == BufferFrame::STATE::HOT;
            // BMC::global_bf->readPageSync(pid, n_guard.bf->page);
            // assert(key == *obt_key);
            std::cout << i << " count: " << c_guard->count << " is_leaf: " << c_guard->is_leaf
                      << " key: " << obt_key << " length: " << key_len << " bf: " << c_guard.bf
                      << " bf_hot: " << bf_is_hot << std::endl;
            // std::cout << i <<  " count: " << n_guard->count << " is_leaf: " << n_guard->is_leaf
            // << " key: " << obt_key
            //           << " length: " << n_guard->getKeyLen(0) << " bf: " << n_guard.bf << "
            //           bf_hot: " << bf_is_hot << std::endl;
            // std::cout << " count " << c_guard->count << " is_leaf: " << c_guard->is_leaf << "
            // key: " << c_guard->getKey(0)  << " length: " << c_guard->getKeyLen(0) << " bf: " <<
            // c_guard.bf << std::endl;
            c_guard.unlock ();
            n_guard.unlock ();
            jumpmu_break;
         }
         jumpmuCatch () {
            failcount++;
            std::cout << "Failed to load the pid: " << secondary_mapping_pid[i].second << std::endl;
          }
      }
   }
   }
   */
   // while(true)
   /*
   {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> target_guard (meta_node_bf);
         size_t spline_idx = 1;
         std::cout << "Trying to attach segments" << std::endl;
         attach_spline (target_guard, spline_idx, height);
         std::cout << "Attaching segments completed" << std::endl;
         HybridPageGuard<BTreeNode> new_target_guard (meta_node_bf);
         // examine_spline_idx(new_target_guard, height);
         // jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES();
      }
   }
   */
   return;
}
OP_RESULT BTreeLL::examine_spline_idx(HybridPageGuard<BTreeNode>& target_guard, int height)
{
   // target_guard.unlock();
   // HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
   // p_guard = HybridPageGuard<BTreeNode>(target_guard, target_guard->upper);
   if (!target_guard->is_leaf) {
      for (auto i = 0; i < target_guard->count; i++) {
         u8 buffer[255];
         target_guard->copyFullKey(i, buffer);
         auto key_len = target_guard->getFullKeyLen(i);
         auto key = utils::u8_to<KEY>(buffer, key_len);
         Swip<BTreeNode>& c_swip = target_guard->getChild(i);
         // p_guard = std::move(target_guard);
         auto c_guard = HybridPageGuard(target_guard, c_swip);
         examine_spline_idx(c_guard, height - 1);
         // std::cout << " key: " << key << " key_len: " << key_len << " seg_ptr: " << target_guard->slot[i].seg_ptr << " height: " << height <<
         // std::endl; c_guard.unlock();
      };
      // Swip<BTreeNode>& c_swip = target_guard->getChild(target_guard->count);
      Swip<BTreeNode>& c_swip = target_guard->upper;
      auto c_guard = HybridPageGuard(target_guard, c_swip);
      examine_spline_idx(c_guard, height - 1);
   }
   // target_guard.unlock();
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
void BTreeLL::stats()
{
   auto height = getHeight();
   printf("[Stats] Height: %lu\n", height);
#ifdef SMO_STATS
   printf("[Stats] SMO incorrect_leaf: %lu num_splits: %lu\n", incorrect_leaf, num_splits);
#endif

#ifdef INMEM
   auto entries = countEntries();
   auto pages = countPages();
   // auto height = getHeight();
   auto inner_pages = countInner();
   auto leaf_pages = pages - inner_pages;
   printf("[Stats] Entries: %lu Pages: %lu Inner: %lu Leaf: %lu Height: %lu\n", entries, pages, inner_pages, leaf_pages, height);

   auto fanout_count = [](BTreeNode& page) { return page.count; };
   auto ignore = [](BTreeNode&) { return 0; };
   auto average_inner_fanout = iterateAllPages(fanout_count, ignore) / inner_pages;
   auto average_leaf_fanout = iterateAllPages(ignore, fanout_count) / leaf_pages;
   printf("[Stats] Average fanout of inner node: %lu\n", average_inner_fanout);
   printf("[Stats] Average fanout of leaf node: %lu\n", average_leaf_fanout);
#ifdef DUMP_EACH_PAGE_FANOUT
   // auto count = [](BTreeNode&) { return 1; };
   // auto entries = iterateAllPagesWithoutCheck(ignore, fanout_count, LATCH_FALLBACK_MODE::SPIN);
   // std::cout << "No of entries: " << entries << std::endl;
   // auto total_pages = iterateAllPagesWithoutCheck(count, count, LATCH_FALLBACK_MODE::SPIN);
   // std::cout << "No of pages: " << total_pages << std::endl;
   // auto inner_pages = iterateAllPagesWithoutCheck(count, ignore, LATCH_FALLBACK_MODE::SPIN);
   // std::cout << "Inner pages: " << inner_pages << std::endl;
   // auto leaf_pages = total_pages - inner_pages;
   // std::cout << "Leaf pages: " << leaf_pages << std::endl;
   // //---------------------------------------------------
   // /*
   auto inner_fanout_display_and_count = [](BTreeNode& page) {
      printf("[Stats] inner_fanout: %lu\n", page.count);
      return page.count;
   };
   auto leaf_fanout_display_and_count = [](BTreeNode& page) {
      printf("[Stats] leaf_fanout: %lu\n", page.count);
      return page.count;
   };
   // */
   // auto average_inner_fanout = iterateAllPagesWithoutCheck(fanout_count, ignore, LATCH_FALLBACK_MODE::SPIN) / inner_pages;
   // auto average_leaf_fanout = iterateAllPagesWithoutCheck(ignore, fanout_count, LATCH_FALLBACK_MODE::SPIN) / leaf_pages;
   // std::cout << "Average fanout of inner node: " << average_inner_fanout << std::endl;
   // std::cout << "Average fanout of leaf node: " << average_leaf_fanout << std::endl;
   // //-----------------------------------------------
   iterateAllPages(inner_fanout_display_and_count, leaf_fanout_display_and_count);
#endif
#endif
}

OP_RESULT BTreeLL::attach_segment(const size_t& segment_ptr)
{
   volatile u32 mask = 1;
   auto height = getHeight();
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
         auto target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
         // -------------------------------------------------------------------------------------
         u16 volatile level = 0;
         constexpr auto key_length = sizeof(KEY);
         u8 lower_key[key_length];
         u8 upper_key[key_length];
         // *reinterpret_cast<u64*>(upper_key) = __builtin_bswap64(spline_predictor.spline_points_[segment_ptr].x);
         // *reinterpret_cast<u64*>(lower_key) = __builtin_bswap64(spline_predictor.spline_points_[segment_ptr - 1].x);
         auto upper_key_len = fold(upper_key, spline_predictor.spline_points_[segment_ptr].x);
         auto lower_key_len = fold(lower_key, spline_predictor.spline_points_[segment_ptr - 1].x);
         // -------------------------------------------------------------------------------------
         while (!target_guard->is_leaf) {
            if ((height - level) < min_attach_level_) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
            // -------------------------------------------------------------------------------------
#ifdef ATTACH_AT_ROOT_NODE
            auto pid = target_guard.bf->header.pid;
            auto seg_ptr = attached_segments.find(pid);
            if (seg_ptr != attached_segments.end()) {
               auto& vec = seg_ptr->second;
               vec.push_back(segment_ptr);
               jumpmu_return OP_RESULT::OK;
            } else {
               attached_segments.insert(std::make_pair(pid, std::vector<size_t>{segment_ptr}));
               jumpmu_return OP_RESULT::OK;
            }
#else
            // Mynote: looks for the child pointer
            // auto lower_key = reinterpret_cast<u8*>(&(spline_predictor.spline_points_[segment_ptr-1].x));
            // auto upper_key = reinterpret_cast<u8*>(&(spline_predictor.spline_points_[segment_ptr].x));
            auto lower_pos = target_guard->lowerBound<false>(lower_key, key_length);
            auto upper_pos = target_guard->lowerBound<false>(upper_key, key_length);
            if (lower_pos != upper_pos) {
               auto pid = target_guard.bf->header.pid;
               auto seg_ptr = attached_segments.find(pid);
               if (seg_ptr != attached_segments.end()) {
                  auto& vec = seg_ptr->second;
                  vec.push_back(segment_ptr);
                  DEBUG_BLOCK()
                  {
                     std::cout << "Found pid: " << pid << " already has segment. Attaching segment: " << segment_ptr
                               << " distance from root: " << level << std::endl;
                  }
                  jumpmu_return OP_RESULT::OK;
               } else {
                  std::vector<size_t> vec{segment_ptr};
                  attached_segments.insert(std::make_pair(pid, std::move(vec)));
                  target_guard.bf->header.attached_segments = &(attached_segments[pid]);
                  DEBUG_BLOCK()
                  {
                     std::cout << "Inserting pid: " << pid << " to have segment: " << segment_ptr << " distance from root: " << level << std::endl;
                  }
                  jumpmu_return OP_RESULT::OK;
               }
            } else {
               p_guard = std::move(target_guard);
               auto c_swip = target_guard->upper;
               if (lower_pos < target_guard->count) {
                  c_swip = target_guard->getChild(lower_pos);
               }
               if (level != height - 1) {
                  target_guard = HybridPageGuard(p_guard, c_swip);
               } else {
                  target_guard = HybridPageGuard(p_guard, c_swip);
               }
               level++;
            }
#endif
         }
         p_guard.unlock();
         // -------------------------------------------------------------------------------------
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES();
      }
   };
}

OP_RESULT BTreeLL::attach_spline(HybridPageGuard<BTreeNode>& target_guard, size_t& spline_index, int height)
{
   // target_guard.unlock();
   // HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
   // p_guard = HybridPageGuard<BTreeNode>(target_guard, target_guard->upper);
   if (!target_guard->is_leaf) {
      for (auto i = 0; i < target_guard->count; i++) {
         u8 buffer[255];
         target_guard->copyFullKey(i, buffer);
         auto key_len = target_guard->getFullKeyLen(i);
         auto key = utils::u8_to<KEY>(buffer, key_len);
         Swip<BTreeNode>& c_swip = target_guard->getChild(i);
         // p_guard = std::move(target_guard);
         jumpmuTry()
         {
            auto c_guard = HybridPageGuard(target_guard, c_swip);
            attach_spline(c_guard, spline_index, height - 1);
         }
         jumpmuCatch()
         {
            // MyNote: ignoring.
         }
         while (!spline_predictor.is_within(key, spline_index)) {
            spline_index++;
            if (spline_index >= spline_predictor.GetSize()) {
               std::cout << "spline index greater than no of splines: " << spline_index << std::endl;
               return OP_RESULT::OK;
            }
         }
         {
            // target_guard.toExclusive();
            // target_guard->slot[i].seg_ptr = spline_index;
            // target_guard.incrementGSN();
            // std::cout << "Attaching segment: " << spline_index << " to key: " << key << " key_len: " << key_len << " seg_ptr: " <<
            // target_guard->slot[i].seg_ptr << " height: " << height << std::endl;
            std::cout << "Attaching segment: " << spline_index << " to key: " << key << " key_len: " << key_len << std::endl;
            auto pid = target_guard.bf->header.pid;
            auto itr = attached_segments.find(pid);
            if (itr != attached_segments.end()) {
               auto vec = itr->second;
               vec.push_back(spline_index);
            } else {
               attached_segments.insert(std::make_pair(target_guard.bf->header.pid, std::vector{spline_index}));
            }
            // target_guard.unlock();
         }
      };
      // Swip<BTreeNode>& c_swip = target_guard->getChild(target_guard->count);
      Swip<BTreeNode>& c_swip = target_guard->upper;
      auto c_guard = HybridPageGuard(target_guard, c_swip);
      attach_spline(c_guard, spline_index, height - 1);
   } else {
      std::cout << "Encountered leaf node" << std::endl;
   }
   target_guard.unlock();
   return OP_RESULT::OK;
}

// -------------------------------------------------------------------------------------
u64 BTreeLL::countEntries()
{
   return BTreeGeneric::countEntries();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countPages()
{
   return BTreeGeneric::countPages();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::getHeight()
{
   return BTreeGeneric::getHeight();
}
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
void BTreeLL::undo(void*, const u8*, const u64)
{
   // TODO: undo for storage
}
// -------------------------------------------------------------------------------------
void BTreeLL::todo(void*, const u8*, const u64) {}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BTreeLL::serialize(void* btree_object)
{
   return BTreeGeneric::serialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)));
}
// -------------------------------------------------------------------------------------
void BTreeLL::deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized)
{
   BTreeGeneric::deserialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), serialized);
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeLL::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo,
                                    .todo = todo,
                                    .serialize = serialize,
                                    .deserialize = deserialize};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParent(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
}
}  // namespace btree
}  // namespace storage
}  // namespace leanstore