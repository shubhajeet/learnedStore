#include <gtest/gtest.h>
#include <leanstore/utils/binarySearchSIMD.hpp>
#include <vector>
#ifdef __AVX512F__
// Test Fixture Class
class BinarySearchSIMDFixture : public ::testing::Test
{
  protected:
   void SetUp() override
   {
      arr_uint64 = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
      arr_uint32 = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
   }

  public:
   // Test data
   std::vector<uint64_t> arr_uint64;
   std::vector<uint32_t> arr_uint32;
};

// Test cases for uint64_t version
TEST_F(BinarySearchSIMDFixture, Uint64_TargetPresent)
{
   uint64_t target = 50;
   int result = binarySearchSIMD(arr_uint64, 4, 1, target);
   EXPECT_EQ(result, 4);
}

TEST_F(BinarySearchSIMDFixture, Uint64_TargetNotPresent)
{
   uint64_t target = 35;
   int result = binarySearchSIMD(arr_uint64, 4, 1, target);
   EXPECT_EQ(result, 3);
}

TEST_F(BinarySearchSIMDFixture, Uint64_TargetSmallerThanMin)
{
   uint64_t target = 5;
   int result = binarySearchSIMD(arr_uint64, 4, 1, target);
   EXPECT_EQ(result, 0);
}

TEST_F(BinarySearchSIMDFixture, Uint64_TargetLargerThanMax)
{
   uint64_t target = 120;
   int result = binarySearchSIMD(arr_uint64, 8, 4, target);
   EXPECT_EQ(result, 10);
}

// Test cases for uint32_t version
TEST_F(BinarySearchSIMDFixture, Uint32_TargetPresent)
{
   uint32_t target = 50;
   int result = binarySearchSIMD(arr_uint32, 4, 1, target);
   EXPECT_EQ(result, 4);
}
TEST_F(BinarySearchSIMDFixture, Uint32_TargetNotPresent)
{
   uint32_t target = 35;
   int result = binarySearchSIMD(arr_uint32, 4, 1, target);
   EXPECT_EQ(result, 3);
}

TEST_F(BinarySearchSIMDFixture, Uint32_TargetSmallerThanMin)
{
   uint32_t target = 5;
   int result = binarySearchSIMD(arr_uint32, 4, 1, target);
   EXPECT_EQ(result, 0);
}

TEST_F(BinarySearchSIMDFixture, Uint32_TargetLargerThanMax)
{
   uint32_t target = 120;
   int result = binarySearchSIMD(arr_uint32, 8, 4, target);
   EXPECT_EQ(result, 10);
}
#endif

TEST(BinarySearchTest, ExistingElement)
{
   std::vector<int> arr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
   int predict = 4;
   int maxerror = 1;
   int target = 5;
   int expectedIndex = 4;
   int actualIndex = binarySearch(arr, predict, maxerror, target);
   EXPECT_EQ(expectedIndex, actualIndex);
}

TEST(BinarySearchTest, NonExistingElement)
{
   std::vector<int> arr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
   int predict = 10;
   int maxerror = 1;
   int target = 11;
   int expectedIndex = 10;
   int actualIndex = binarySearch(arr, predict, maxerror, target);
   EXPECT_EQ(expectedIndex, actualIndex);
}
