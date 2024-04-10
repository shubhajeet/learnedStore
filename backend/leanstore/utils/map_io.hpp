#pragma once
#include <tbb/concurrent_unordered_map.h>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <type_traits>
#include <vector>
#include "flat_hash_map.hpp"

template <typename Key, typename Value>
static void store_map(const std::string& file_path, const std::unordered_map<Key, Value>& map_to_store)
{
   std::ofstream outfile(file_path, std::ios::binary);
   if (outfile) {
      // Write the number of elements in the map as the first 8 bytes
      uint64_t map_size = map_to_store.size();
      outfile.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

      // Write each key-value pair as uint64_t key and a vector of size_t values
      for (const auto& pair : map_to_store) {
         const Key& key = pair.first;
         outfile.write(reinterpret_cast<const char*>(&key), sizeof(key));

         const auto& values = pair.second;
         uint64_t num_values = values.size();
         outfile.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));

         outfile.write(reinterpret_cast<const char*>(values.data()), num_values * sizeof(typename Value::value_type));
      }

      std::cout << "Stored " << map_size << " elements to " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for writing" << std::endl;
   }
}
template <typename Key, typename Value>
static void store_map(const std::string& file_path, const std::map<Key, Value>& map_to_store)
{
   std::ofstream outfile(file_path, std::ios::binary);
   if (outfile) {
      // Write the number of elements in the map as the first 8 bytes
      uint64_t map_size = map_to_store.size();
      outfile.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

      // Write each key-value pair as uint64_t key and a vector of size_t values
      for (const auto& pair : map_to_store) {
         const Key& key = pair.first;
         outfile.write(reinterpret_cast<const char*>(&key), sizeof(key));

         const auto& values = pair.second;
         uint64_t num_values = values.size();
         outfile.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));

         outfile.write(reinterpret_cast<const char*>(values.data()), num_values * sizeof(typename Value::value_type));
      }

      std::cout << "Stored " << map_size << " elements to " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for writing" << std::endl;
   }
}

template <typename Key, typename Value>
static void store_map(const std::string& file_path, const ska::flat_hash_map<Key, Value>& map_to_store)
{
   std::ofstream outfile(file_path, std::ios::binary);
   if (outfile) {
      // Write the number of elements in the map as the first 8 bytes
      uint64_t map_size = map_to_store.size();
      outfile.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

      // Write each key-value pair as uint64_t key and a vector of size_t values
      for (const auto& pair : map_to_store) {
         const Key& key = pair.first;
         outfile.write(reinterpret_cast<const char*>(&key), sizeof(key));

         const auto& values = pair.second;
         uint64_t num_values = values.size();
         outfile.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));

         outfile.write(reinterpret_cast<const char*>(values.data()), num_values * sizeof(typename Value::value_type));
      }

      std::cout << "Stored " << map_size << " elements to " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for writing" << std::endl;
   }
}

template <typename Key, typename Value>
static void store_map(const std::string& file_path, const tbb::concurrent_unordered_map<Key, Value>& map_to_store)
{
   std::ofstream outfile(file_path, std::ios::binary);
   if (outfile) {
      // Write the number of elements in the map as the first 8 bytes
      uint64_t map_size = map_to_store.size();
      outfile.write(reinterpret_cast<const char*>(&map_size), sizeof(map_size));

      // Write each key-value pair as uint64_t key and a vector of size_t values
      for (const auto& pair : map_to_store) {
         const Key& key = pair.first;
         outfile.write(reinterpret_cast<const char*>(&key), sizeof(key));

         const auto& values = pair.second;
         uint64_t num_values = values.size();
         outfile.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));

         outfile.write(reinterpret_cast<const char*>(values.data()), num_values * sizeof(typename Value::value_type));
      }

      std::cout << "Stored " << map_size << " elements to " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for writing" << std::endl;
   }
}

template <typename Key, typename Value>
static bool load_map(const std::string& file_path, std::map<Key, Value>& map_to_load)
{
   std::ifstream infile(file_path, std::ios::binary);
   if (infile) {
      // Read the number of elements in the map from the first 8 bytes
      uint64_t map_size;
      infile.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

      // Read each key-value pair and insert it into the map
      for (uint64_t i = 0; i < map_size; ++i) {
         Key key;
         infile.read(reinterpret_cast<char*>(&key), sizeof(key));

         uint64_t num_values;
         infile.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));

         Value values(num_values);
         infile.read(reinterpret_cast<char*>(values.data()), num_values * sizeof(typename Value::value_type));

         map_to_load[key] = std::move(values);
      }

      std::cout << "Loaded " << map_size << " elements from " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for reading" << std::endl;
   }

   return true;
}

template <typename Key, typename Value>
static bool load_map(const std::string& file_path, std::unordered_map<Key, Value>& map_to_load)
{
   std::ifstream infile(file_path, std::ios::binary);
   if (infile) {
      // Read the number of elements in the map from the first 8 bytes
      uint64_t map_size;
      infile.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

      // Read each key-value pair and insert it into the map
      for (uint64_t i = 0; i < map_size; ++i) {
         Key key;
         infile.read(reinterpret_cast<char*>(&key), sizeof(key));

         uint64_t num_values;
         infile.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));

         Value values(num_values);
         infile.read(reinterpret_cast<char*>(values.data()), num_values * sizeof(typename Value::value_type));

         map_to_load[key] = std::move(values);
      }

      std::cout << "Loaded " << map_size << " elements from " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for reading" << std::endl;
   }

   return true;
}

template <typename Key, typename Value>
static bool load_map(const std::string& file_path, ska::flat_hash_map<Key, Value>& map_to_load)
{
   std::ifstream infile(file_path, std::ios::binary);
   if (infile) {
      // Read the number of elements in the map from the first 8 bytes
      uint64_t map_size;
      infile.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

      // Read each key-value pair and insert it into the map
      for (uint64_t i = 0; i < map_size; ++i) {
         Key key;
         infile.read(reinterpret_cast<char*>(&key), sizeof(key));

         uint64_t num_values;
         infile.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));

         Value values(num_values);
         infile.read(reinterpret_cast<char*>(values.data()), num_values * sizeof(typename Value::value_type));

         map_to_load[key] = std::move(values);
      }

      std::cout << "Loaded " << map_size << " elements from " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for reading" << std::endl;
   }

   return true;
}

template <typename Key, typename Value>
static bool load_map(const std::string& file_path, tbb::concurrent_unordered_map<Key, Value>& map_to_load)
{
   std::ifstream infile(file_path, std::ios::binary);
   if (infile) {
      // Read the number of elements in the map from the first 8 bytes
      uint64_t map_size;
      infile.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

      // Read each key-value pair and insert it into the map
      for (uint64_t i = 0; i < map_size; ++i) {
         Key key;
         infile.read(reinterpret_cast<char*>(&key), sizeof(key));

         uint64_t num_values;
         infile.read(reinterpret_cast<char*>(&num_values), sizeof(num_values));

         Value values(num_values);
         infile.read(reinterpret_cast<char*>(values.data()), num_values * sizeof(typename Value::value_type));

         map_to_load[key] = std::move(values);
      }

      std::cout << "Loaded " << map_size << " elements from " << file_path << std::endl;
   } else {
      std::cerr << "Error: failed to open " << file_path << " for reading" << std::endl;
   }
   return true;
}

template <typename Key, typename Value>
void dump_map(const tbb::concurrent_unordered_map<Key, Value>& map_to_dump)
{
   std::cout << "Contents of map:" << std::endl;
   for (const auto& pair : map_to_dump) {
      const Key& key = pair.first;
      const auto& values = pair.second;
      std::cout << key << ": [ ";
      for (const auto& value : values) {
         std::cout << value << " ";
      }
      std::cout << "]" << std::endl;
   }
}

template <typename Key, typename Value>
void dump_map(const ska::flat_hash_map<Key, Value>& map_to_dump)
{
   std::cout << "Contents of map:" << std::endl;
   for (const auto& pair : map_to_dump) {
      const Key& key = pair.first;
      const auto& values = pair.second;
      std::cout << key << ": [ ";
      for (const auto& value : values) {
         std::cout << value << " ";
      }
      std::cout << "]" << std::endl;
   }
}
