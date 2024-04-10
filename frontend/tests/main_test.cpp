#include <gtest/gtest.h>
#include <tbb/tbb.h>

int main(int argc, char** argv)
{
   testing::InitGoogleTest(&argc, argv);
   // tbb::task_scheduler_init taskScheduler(2);

   int ret = RUN_ALL_TESTS();
   return ret;
}
