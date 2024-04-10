#include <iostream>
#include <stdexcept>

template <typename T>
class DynamicArray
{
  private:
   T* data;
   size_t size_;
   size_t capacity_;

  public:
   // Constructor
   DynamicArray() : data(nullptr), size_(0), capacity_(0) {}

   // Destructor
   ~DynamicArray() { delete[] data; }

   // Copy constructor
   DynamicArray(const DynamicArray& other) : data(nullptr), size_(0), capacity_(0) { copyFrom(other); }

   // Assignment operator
   DynamicArray& operator=(const DynamicArray& other)
   {
      if (this != &other) {
         delete[] data;
         copyFrom(other);
      }
      return *this;
   }

   // Accessor methods
   size_t size() const { return size_; }

   size_t capacity() const { return capacity_; }

   bool empty() const { return size_ == 0; }

   T& operator[](size_t index)
   {
      if (index >= size_) {
         throw std::out_of_range("Index out of range");
      }
      return data[index];
   }

   const T& operator[](size_t index) const
   {
      if (index >= size_) {
         throw std::out_of_range("Index out of range");
      }
      return data[index];
   }

   T& at(size_t index) { return operator[](index); }

   const T& at(size_t index) const { return operator[](index); }

   T& front()
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      return data[0];
   }

   const T& front() const
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      return data[0];
   }

   T& back()
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      return data[size - 1];
   }

   const T& back() const
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      return data[size - 1];
   }

   // Modifier methods
   void push_back(const T& value)
   {
      if (size_ >= capacity_) {
         size_t newCapacity = (capacity_ == 0) ? 1 : capacity_ * 2;
         reserve(newCapacity);
      }
      data[size_++] = value;
   }

   void pop_back()
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      --size_;
   }

   void clear() { size_ = 0; }

   void reserve(size_t newCapacity)
   {
      if (newCapacity > capacity_) {
         T* newData = new T[newCapacity];
         for (size_t i = 0; i < size_; ++i) {
            newData[i] = data[i];
         }
         delete[] data;
         data = newData;
         capacity_ = newCapacity;
      }
   }

   void resize(size_t newCapacity, const T& defaultValue = 0)
   {
      if (newCapacity > capacity_) {
         T* newData = new T[newCapacity];
         for (size_t i = 0; i < size_; ++i) {
            newData[i] = data[i];
         }
         for (size_t i = size_; i < newCapacity; ++i) {
            newData[i] = defaultValue;
         }
         delete[] data;
         data = newData;
         capacity_ = newCapacity;
         size_ = newCapacity;
      }
   }

  private:
   // Helper function to copy from another array
   void copy_from(const DynamicArray& other)
   {
      data = new T[other.size_];
      size_ = other.size_;
      capacity_ = other.size_;
      for (size_t i = 0; i < size_; ++i) {
         data[i] = other.data[i];
      }
   }
};