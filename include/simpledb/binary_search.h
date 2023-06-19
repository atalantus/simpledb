#pragma once
#include <bit>
#include <functional>

namespace simpledb {

template <typename It, typename T, typename Compare = std::less<>>
size_t lower_bound_branchless(It low, It up, const T& val, Compare cmp = {}) {
   size_t l = up - low;
   if (l == 0)
      return 0;
   size_t i = 0;

   while (auto half = l / 2) {
      auto mid = low + i;
      std::advance(mid, half);
      i = cmp(*mid, val) ? i + half : i;
      l -= half;
   }
   if (cmp(*(low + i), val))
      ++i;
   return i;
}

}