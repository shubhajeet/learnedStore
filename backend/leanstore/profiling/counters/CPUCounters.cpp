#include "CPUCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
   // MyNote:: mutex for unordered_map i.e. hashmap
std::mutex CPUCounters::mutex; 
   // MyNote:: No of registered page provider thread
u64 CPUCounters::id = 0;
   // MyNote:: tracks information about page provider threads
std::unordered_map<u64, CPUCounters> CPUCounters::threads; 
// -------------------------------------------------------------------------------------
u64 CPUCounters::registerThread(string name, bool perf_inherit)
{
   std::unique_lock guard(mutex);
   threads[id] = {.e = std::make_unique<PerfEvent>(perf_inherit), .name = name};
   return id++;
}
void CPUCounters::removeThread(u64 id)
{
   std::unique_lock guard(mutex);
   threads.erase(id);
}
}  // namespace leanstore
