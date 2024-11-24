# ---------------------------------------------------------------------------
# Leanstore DB
# ---------------------------------------------------------------------------

project(leanstoredb)
cmake_minimum_required(VERSION 3.7)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
enable_language(ASM)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -rdynamic -march=native")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64 -march=native) # -fno-elide-constructors no need for now

# add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64 -mavx512f -mavx512bw -march=native) # -fno-elide-constructors no need for now
else()
  add_compile_options(-Wall -Wextra -march=native)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3") # because we had weird behavior on aws ARM

# Can possibly have performance impact disable in production
# if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftrapv")
# elseif(MSVC)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Qtrapv")
# endif()
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
  add_compile_options(-fstandalone-debug)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(APPLE)
  list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/bison)
  list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/flex)
endif(APPLE)

if(CYGWIN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
endif(CYGWIN)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
# Add btree used by RMI
# execute_process(COMMAND cat /proc/cpuinfo OUTPUT_VARIABLE CPUINFO)
# string(REGEX MATCH "cpu MHz[\\s]*:[\\s]*([0-9]+\\.[0-9]+)" CPUFREQ ${CPUINFO})
# set(CPU_FREQUENCY ${CMAKE_MATCH_1})
# add_compile_definitions(CPU_FREQUENCY=${CPU_FREQUENCY})
set(MYLIBRARY_DIR submodules/cppInstrumentation)
include_directories(${MYLIBRARY_DIR}/include)
link_directories(${MYLIBRARY_DIR}/build_Release)
include_directories(submodules/flat_hash_map)

# include_directories(~/projects/RadixSpline/include)
# include_directories(~/projects/rmi/include)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SPARSEHASH REQUIRED libsparsehash)

include_directories(${SPARSEHASH_INCLUDE_DIRS})
link_directories(${SPARSEHASH_LIBRARY_DIRS})

# include_directories(${SPARSEHASH_INCLUDE_DIR})
find_package(Threads REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)

include("${CMAKE_SOURCE_DIR}/libs/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/libs/tabluate.cmake")
include("${CMAKE_SOURCE_DIR}/libs/rapidjson.cmake")

# include("${CMAKE_SOURCE_DIR}/libs/googletest.cmake")

# include("${CMAKE_SOURCE_DIR}/libs/benchmark.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/yaml-cpp.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/fastpfor.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/spdlog.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/tbb.cmake")

# include("${CMAKE_SOURCE_DIR}/libs/psql.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/gdouble.cmake")
# include("${CMAKE_SOURCE_DIR}/libs/turbo.cmake")
# nclude("${CMAKE_SOURCE_DIR}/libs/lz4.cmake")

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------
include("${CMAKE_SOURCE_DIR}/shared-headers/local.cmake")

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
add_subdirectory("backend")

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------
add_subdirectory("frontend")
