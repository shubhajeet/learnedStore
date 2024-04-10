add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)

add_executable(test test/test.cpp)
target_link_libraries(test leanstore Threads::Threads)
target_include_directories(test PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(ycsb ycsb/ycsb.cpp)
target_link_libraries(ycsb leanstore Threads::Threads)
target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(benchmark_ycsb ycsb/benchmark_ycsb.cpp)
target_link_libraries(benchmark_ycsb leanstore Threads::Threads)
target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads)
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# https://source.wiredtiger.com/10.0.0/build-posix.html
# add_executable(wiredtiger_tpcc tpc-c/wiredtiger_tpcc.cpp)
# target_link_libraries(wiredtiger_tpcc leanstore wiredtiger Threads::Threads dl z libbz2.a lz4 snappy)
# target_include_directories(wiredtiger_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# #####################################################################################
# find_package(GTest 1.11 QUIET MODULE REQUIRED)

# if(DEFINED GTest_VERSION)
# set(gtestlib GTest::gtest)
# else()
# find_library(gtestlib gtest)

# if(${gtestlib} STREQUAL gtestlib-NOTFOUND)
# message(FATAL_ERROR "Can not find GTest")
# endif()
# endif()

# message(STATUS "Using GTest library: ${gtestlib}")

# add_executable(leanstore_test tests/leanstore_test.cc)
# target_link_libraries(leanstore_test leanstore Threads::Threads ${gtestlib})
# target_include_directories(leanstore_test PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(benchmark_ycsb_test tests/benchmark_ycsb_test.cc)
# target_link_libraries(benchmark_ycsb_test leanstore Threads::Threads ${gtestlib})
# target_include_directories(benchmark_ycsb_test PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_include_directories(benchmark_ycsb_test PRIVATE ycsb)
add_subdirectory(tests)