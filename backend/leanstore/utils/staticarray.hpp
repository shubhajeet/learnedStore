#include <iostream>
#include <stdexcept>

template <typename T, size_t N>
class StaticArray
{
  private:
   T data[N];
   size_t size_;

  public:
   // Constructor
   StaticArray() : size_(0) {}

   // Accessor methods
   size_t size() const { return N; }

   size_t capacity() const { return N; }

   bool empty() const { return size_ == 0; }

   T& operator[](size_t index)
   {
      if (index >= N) {
         throw std::out_of_range("Index out of range");
      }
      return data[index];
   }

   const T& operator[](size_t index) const
   {
      if (index >= N) {
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
      return data[size_ - 1];
   }

   const T& back() const
   {
      if (empty()) {
         throw std::out_of_range("Array is empty");
      }
      return data[size_ - 1];
   }

   // Modifier methods
   void push_back(const T& value)
   {
      if (size_ >= N) {
         throw std::out_of_range("Array is full");
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

   void resize(size_t size, const T& value = 0)
   {
      if (size_ > N) {
         throw std::out_of_range("Size is greater than capacity");
      }
      size_ = size;
   }
};