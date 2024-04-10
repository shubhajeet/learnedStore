# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
        flat_hash_map_src
        PREFIX "vendor/flat_hash_map"
        GIT_REPOSITORY "https://github.com/skarupke/flat_hash_map.git"
        TIMEOUT 10
        BUILD_COMMAND make
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        INSTALL_COMMAND ""
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/tbb_cpp
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

# Prepare json
ExternalProject_Get_Property(flat_hash_map_src source_dir)

set(FLAT_HASH_MAP_INCLUDE_PATH ${source_dir}/include)

file(MAKE_DIRECTORY ${FLAT_HASH_MAP_INCLUDE_PATH})

add_library(FLAT_HASH_MAP SHARED IMPORTED)

set_property(TARGET FLAT_HASH_MAP PROPERTY IMPORTED_LOCATION ${FLAT_HASH_MAP_INCLUDE_PATH})
