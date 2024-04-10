find_package(TBB REQUIRED)
find_package(GTest 1.11 QUIET MODULE REQUIRED)

if(DEFINED GTest_VERSION)
    set(gtestlib GTest::gtest)
else()
    find_library(gtestlib gtest)

    if(${gtestlib} STREQUAL gtestlib-NOTFOUND)
        message(FATAL_ERROR "Can not find GTest")
    endif()
endif()

message(STATUS "Using GTest library: ${gtestlib}")

# Discover and run all tests in the directory
file(GLOB_RECURSE TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp")
add_executable(test_all ${TEST_SOURCES})
add_dependencies(test_all leanstore)
target_link_libraries(test_all leanstore Threads::Threads ${gtestlib} TBB::tbb)
target_include_directories(test_all PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(test_all PRIVATE ../ycsb)