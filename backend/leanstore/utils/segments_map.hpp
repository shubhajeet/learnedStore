#ifndef SEGMENTS_MAP_HPP
#define SEGMENTS_MAP_HPP

#include <fstream>
#include <iostream>
#include <map>
#include <vector>

template <typename KeyType>
struct Segments {
   std::vector<size_t> idxVec;
   std::vector<KeyType> keyVec;
   bool insert(const KeyType& key, const size_t value)
   {
      keyVec.push_back(key);
      idxVec.push_back(value);
   }
};

template <typename PIDType, typename KeyType, typename ValueType>
class SegmentsMap
{
  public:
   void storeToFile(const std::string& filename, const std::map<PIDType, Segments<KeyType, ValueType>>& SegmentsMap) const
   {
      std::ofstream file(filename, std::ios::binary);
      if (!file) {
         std::cerr << "Failed to open file for writing: " << filename << std::endl;
         return;
      }

      size_t mapSize = segmentsMap.size();
      file.write(reinterpret_cast<const char*>(&mapSize), sizeof(mapSize));

      for (const auto& entry : segmentsMap) {
         const KeyType& key = entry.first;
         const Segments<KeyType, ValueType>& segments = entry.second;

         file.write(reinterpret_cast<const char*>(&key), sizeof(key));

         size_t sizeVecSize = segments.sizeVec.size();
         file.write(reinterpret_cast<const char*>(&sizeVecSize), sizeof(sizeVecSize));
         file.write(reinterpret_cast<const char*>(segments.sizeVec.data()), sizeof(size_t) * sizeVecSize);

         size_t keyVecSize = segments.keyVec.size();
         file.write(reinterpret_cast<const char*>(&keyVecSize), sizeof(keyVecSize));
         file.write(reinterpret_cast<const char*>(segments.keyVec.data()), sizeof(ValueType) * keyVecSize);
      }

      file.close();
   }
   void loadFromFile(const std::string& filename, std::map<PIDType, Segments<KeyType, ValueType>>& segmentsMap)
   {
      std::ifstream file(filename, std::ios::binary);
      if (!file) {
         std::cerr << "Failed to open file for reading: " << filename << std::endl;
         return;
      }

      segmentsMap.clear();

      size_t mapSize;
      file.read(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));

      for (size_t i = 0; i < mapSize; ++i) {
         KeyType key;
         file.read(reinterpret_cast<char*>(&key), sizeof(key));

         size_t sizeVecSize;
         file.read(reinterpret_cast<char*>(&sizeVecSize), sizeof(sizeVecSize));
         std::vector<size_t> sizeVec(sizeVecSize);
         file.read(reinterpret_cast<char*>(sizeVec.data()), sizeof(size_t) * sizeVecSize);

         size_t keyVecSize;
         file.read(reinterpret_cast<char*>(&keyVecSize), sizeof(keyVecSize));
         std::vector<ValueType> keyVec(keyVecSize);
         file.read(reinterpret_cast<char*>(keyVec.data()), sizeof(ValueType) * keyVecSize);

         Segments<KeyType, ValueType> segments;
         segments.sizeVec = sizeVec;
         segments.keyVec = keyVec;

         segmentsMap[key] = segments;
      }

      file.close();
   }
};
#endif  // SEGMENTS_MAP_HPP
