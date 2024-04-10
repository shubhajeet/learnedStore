#include "leanstore/utils/map_io.hpp"
#include <vector>
#include "gtest/gtest.h"

// Define a test fixture to create a map for testing
class ConcurrentUnorderedMapTest : public ::testing::Test
{
  public:
   tbb::concurrent_unordered_map<uint64_t, std::vector<size_t>> map;
   ConcurrentUnorderedMapTest()
   {
      map[1] = {1, 2, 3};
      map[2] = {4, 5, 6};
      map[3] = {7, 8, 9};
   }
};

// Test saving and loading a map
TEST_F(ConcurrentUnorderedMapTest, SaveAndLoad)
{
   // Save the map to a file
   std::string filename = "test_map.bin";
   store_map(filename, map);

   // Load the map from the file
   tbb::concurrent_unordered_map<uint64_t, std::vector<size_t>> new_map;
   load_map(filename, new_map);

   // Compare the original and loaded maps
   EXPECT_EQ(map.size(), new_map.size());
   for (const auto& [key, value] : map) {
      auto it = new_map.find(key);
      EXPECT_TRUE(it != new_map.end());
      EXPECT_EQ(value, it->second);
   }
}

// Test dumping a map to stdout
TEST_F(ConcurrentUnorderedMapTest, DumpToStdout)
{
   // Redirect stdout to a stringstream
   std::stringstream ss;
   std::streambuf* old_cout_buf = std::cout.rdbuf();
   std::cout.rdbuf(ss.rdbuf());

   // Dump the map to stdout
   dump_map(map);

   // Restore stdout and check the output
   std::cout.rdbuf(old_cout_buf);
   std::string output = ss.str();
   // std::cout << output << std::endl;
   EXPECT_TRUE(output.find("1: [ 1 2 3 ]") != std::string::npos);
   EXPECT_TRUE(output.find("2: [ 4 5 6 ]") != std::string::npos);
   EXPECT_TRUE(output.find("3: [ 7 8 9 ]") != std::string::npos);
}
