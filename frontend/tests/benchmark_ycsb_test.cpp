#include <chrono>
#include <filesystem>
#include <iostream>

#include <tbb/tbb.h>
#include "gtest/gtest.h"
#include "logger.h"
#include "test_util.h"

using namespace std;
using namespace util;
class KeyTraceTest : public testing::Test
{
  public:
   KeyTrace<uint64_t>* seq = nullptr;
   KeyTrace<uint64_t>* ran = nullptr;
   KeyTraceTest(){};
   ~KeyTraceTest(){};
   std::string test_file;
   virtual void SetUp()
   {
      seq = new KeyTrace<uint64_t>();
      seq->generate(100, true, nullptr);
      ran = new KeyTrace<uint64_t>();
      ran->generate(100, false, nullptr);
      test_file = "test.csv";
      std::ofstream outfile(test_file, std::ios::out);
      outfile << "1" << std::endl << "2" << std::endl << "3" << std::endl;
      outfile.close();
   };
   virtual void TearDown()
   {
      delete seq;
      delete ran;
      std::remove(test_file.c_str());
   };
};

TEST_F(KeyTraceTest, expectedKeyTest)
{
   for (auto i = 0; i < seq->count_; i++) {
      EXPECT_EQ(i + 1, seq->keys_[i]) << "key at " << i << " should be " << i + 1 << " found to be " << seq->keys_[i] << std::endl;
   }
}
/*
TEST_F (RandomTrace, sortTest) {
    ran->Sort();
    for (auto i = 1; i < ran->count_; i++) {
        EXPECT_LT(ran->keys_[i-1],ran->keys_[i]) << "key at " << i << " should be less than at" << i+1 << " found to be " << ran->keys_[i-1] << " > "
<< ran->keys_[i] << std::endl;
    }
}
*/

// TEST_F(RandomTrace, randomTest)
// {
//    ran->Randomize();
//    auto less = 0;
//    auto greater = 0;
//    for (auto i = 1; i < ran->count_; i++) {
//       if (ran->keys_[i - 1] < ran->keys_[i]) {
//          less++;
//       } else {
//          greater++;
//       }
//    };
//    EXPECT_TRUE(less > 0) << "Is still sorted";
//    EXPECT_TRUE(greater > 0) << "Is still sorted";
// }

TEST_F(KeyTraceTest, WritingToFile)
{
   // Get the path to the system's temporary directory
   const std::filesystem::path temp_dir_path = std::filesystem::temp_directory_path();

   // Create a subdirectory in the temporary directory
   const std::string sub_dir_name = "my_temp_dir";
   const std::filesystem::path sub_dir_path = temp_dir_path / sub_dir_name;
   std::filesystem::create_directory(sub_dir_path);

   // Write some data to a file in the temporary directory
   const std::string filename = sub_dir_path / "temp.txt";
   ran->ToFile(filename);
   auto copy_vec = ran->keys_;

   auto newtrace = new KeyTrace<uint64_t>();
   // newtrace->generate(100, true);
   newtrace->FromFile(filename);
   for (auto i = 0; i < ran->count_; i++) {
      EXPECT_EQ(newtrace->keys_[i], ran->keys_[i]) << "Expecting the written and read keys from trace to be same. But found written " << ran->keys_[i]
                                                   << " read " << newtrace->keys_[i];
   }
}

TEST_F(KeyTraceTest, FromCSV)
{
   auto localtrace = KeyTrace<uint64_t>();
   localtrace.FromCSV(test_file);
   ASSERT_EQ(localtrace.keys_[1], 2);
   ASSERT_EQ(localtrace.keys_[2], 3);
   ASSERT_EQ(localtrace.keys_[0], 1);
}
