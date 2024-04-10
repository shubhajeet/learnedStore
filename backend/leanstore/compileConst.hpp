#pragma once
/**
 * @file   compileConst.hpp
 * @brief  This file contains all the compile time constants. It specifically created fro the instrumentation purpose. However, it can be used for
 * other purposes as well.
 */
// #define TRACK_WITH_HT
// #define COUNT_LEAF_AND_KEY
// #define AVOID_SEGMENT_SEARCH
// #define INSTRUMENT_CODE
// #define LATENCY_BREAKDOWN
// #define USE_TSC
// #define SIMPLE_SIZE 10
// #define INSTRUMENT_CACHE_MISS
#define PID_CHECK
// #define SIMD_SEARCH_HINT
// #define INSERT_MODEL_IN_LEAF_NODE
#define MODEL_IN_LEAF_NODE
#define MODEL_SEG
#define MODEL_RS
#define MODEL_RMI
#define MODEL_LR
#define RS_EXPONENTIAL_SEARCH
#define RMI_EXPONENTIAL_SEARCH
#define LR_EXPONENTIAL_SEARCH
#define EXPONENTIAL_SEARCH
// #define AUTO_TRAIN
#define SMO_STATS
#define COMPACT_MAPPING
// #define SEGMENT_STATS
#define ATTACH_AT_ROOT
#define ATTACH_AT_ROOT_NODE
// #define ATTACH_SEGMENTS_STATS
#define INMEM
// #define DUMP_EACH_PAGE_FANOUT
#define DONT_USE_PID
// #define SIMULATE_BUFFERPOOL_MISS
#define MISS_PROB 0.01

using KEY = uint32_t;
// using KEY = uint64_t;