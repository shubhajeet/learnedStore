#pragma once
#include <fstream>
#include <vector>

template <typename T>
class BinaryFileStorage
{
public:
    BinaryFileStorage(const std::string &fileName) : fileName(fileName) {}
    ~BinaryFileStorage() {}

    void store(const std::vector<T> &vec)
    {
        std::ofstream file(fileName, std::ios::binary | std::ios::out);
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open file for writing");
        }
        size_t vecSize = vec.size();
        file.write(reinterpret_cast<const char *>(&vecSize), sizeof(size_t));
        file.write(reinterpret_cast<const char *>(&vec[0]), vecSize * sizeof(T));
        file.close();
    }

    std::vector<T> load()
    {
        std::ifstream file(fileName, std::ios::binary | std::ios::in);
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open file for reading");
        }
        size_t vecSize = 0;
        file.read(reinterpret_cast<char *>(&vecSize), sizeof(size_t));
        std::vector<T> vec(vecSize);
        file.read(reinterpret_cast<char *>(&vec[0]), vecSize * sizeof(T));
        file.close();
        return vec;
    }

private:
    std::string fileName;
};
