cmake_minimum_required(VERSION 3.12)
project(MyProject)

# Load ExternalProject module
include(ExternalProject)

# Set the URL for sparsehash
set(sparsehash_url "https://github.com/sparsehash/sparsehash/archive/refs/tags/sparsehash-2.0.4.tar.gz")
set(sparsehash_prefix "${CMAKE_BINARY_DIR}/sparsehash")

# ExternalProject_Add for sparsehash
ExternalProject_Add(sparsehash_external
    PREFIX ${sparsehash_prefix}
    URL ${sparsehash_url}

    # Other configuration options can be added here, if needed
    # For example, you can specify build commands using the following options:
    # CONFIGURE_COMMAND <command>
    # BUILD_COMMAND <command>
    # INSTALL_COMMAND <command>
)

# Include sparsehash headers in your project
include_directories(${sparsehash_prefix}/src/sparsehash_external)

# Add your own project here and link it with sparsehash
add_executable(my_project main.cpp)

# Add other source files to the target if necessary
# target_sources(my_project PRIVATE <list_of_source_files>)
target_link_libraries(my_project PRIVATE ${sparsehash_prefix}/src/sparsehash_external-build/sparsehash)

# Add a dependency on the sparsehash_external target
add_dependencies(my_project sparsehash_external)
