#include "simpledb/segment.h"
#include <cmath>

using FSISegment = simpledb::FSISegment;

FSISegment::FSISegment(uint16_t segment_id, BufferManager& buffer_manager, schema::Table& table)
   : Segment(segment_id, buffer_manager),
     linear_factor{static_cast<uint32_t>(buffer_manager.get_page_size() / 16) + 1},
     log_factor{log2f(static_cast<float>(buffer_manager.get_page_size())) / 8.0f},
     free_cache(), table(table) {
   free_cache.fill(invalidPid);

   // initialize cache
   uint64_t curPageIndex = 0;

   while (curPageIndex < table.allocated_pages) {
      auto& bf = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^
                                            (curPageIndex / (buffer_manager.get_page_size() * 2)),
                                         false);

      uint64_t fsiOffset = 0;

      while (fsiOffset < buffer_manager.get_page_size()) {
         // check upper
         uint8_t upper = *reinterpret_cast<uint8_t*>(bf.get_data() + fsiOffset) >> 4;
         //assert(upper == encode_free_space(reinterpret_cast<SlottedPage*>(buffer_manager.fix_page((static_cast<uint64_t>(table.sp_segment) << 48) ^ curPageIndex, false).get_data())->get_free_space()));
         if (free_cache[upper] == invalidPid)
            free_cache[upper] = curPageIndex;
         curPageIndex++;
         if (curPageIndex == table.allocated_pages)
            break;

         // check lower
         uint8_t lower = *reinterpret_cast<uint8_t*>(bf.get_data() + fsiOffset) & 0b00001111;
         //assert(lower == encode_free_space(reinterpret_cast<SlottedPage*>(buffer_manager.fix_page((static_cast<uint64_t>(table.sp_segment) << 48) ^ curPageIndex, false).get_data())->get_free_space()));
         if (free_cache[lower] == invalidPid)
            free_cache[lower] = curPageIndex;
         curPageIndex++;
         if (curPageIndex == table.allocated_pages)
            break;

         fsiOffset++;
      }

      buffer_manager.unfix_page(bf, false);
   }
}

uint8_t FSISegment::encode_free_space(uint32_t free_space) const {
   if (free_space < buffer_manager.get_page_size() / 2) {
      // use logarithmic
      return static_cast<uint8_t>(std::floor(log2f(static_cast<float>(free_space)) / log_factor));
   } else {
      // use linear
      return free_space / linear_factor;
   }
}

uint32_t FSISegment::decode_free_space(uint8_t free_space) const {
   assert(free_space < 16);
   if (free_space < 8) {
      // use logarithmic
      return free_space == 0 ? 0 : static_cast<uint32_t>(std::ceil(std::pow(2, static_cast<float>(free_space) * log_factor)));
   } else {
      // use linear
      return free_space * linear_factor;
   }
}

void FSISegment::update_free_cache(uint64_t pageIndex, uint8_t freeSpace) {
   uint8_t prevFreeSpace = 16;
   for (uint8_t i = 0; i < 16; ++i) {
      if (free_cache[i] == pageIndex) {
         if (i != freeSpace) {
            // we will have to find a new cache entry for the old index
            prevFreeSpace = i;
         }
         break;
      }
   }

   // set new free cache
   if (free_cache[freeSpace] == invalidPid || pageIndex < free_cache[freeSpace]) {
      free_cache[freeSpace] = pageIndex;
   }

   // check old entry
   if (prevFreeSpace < 16) {
      // find the earliest page with same free space as prevFreeSpace

      // since this page was the earliest entry before we can start the search at one page after this
      uint64_t curPageIndex = pageIndex + 1;

      while (curPageIndex < table.allocated_pages) {
         uint64_t fsiIndex = curPageIndex / (buffer_manager.get_page_size() * 2);
         uint64_t fsiOffset = curPageIndex % (buffer_manager.get_page_size() * 2);
         auto& bf = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ fsiIndex, false);
         bool found = false;

         // scan over bitmap page
         while (fsiOffset < buffer_manager.get_page_size() * 2) {
            if (fsiOffset % 2 == 0) {
               // check upper
               uint8_t upper = *reinterpret_cast<uint8_t*>(bf.get_data() + fsiOffset / 2) >> 4;
               if (upper == prevFreeSpace) {
                  // found
                  free_cache[prevFreeSpace] = curPageIndex;
                  found = true;
                  break;
               }
               curPageIndex++;
               if (curPageIndex == table.allocated_pages) break;
            } else {
               // check lower
               uint8_t lower = *reinterpret_cast<uint8_t*>(bf.get_data() + fsiOffset / 2) & 0b00001111;
               if (lower == prevFreeSpace) {
                  // found
                  free_cache[prevFreeSpace] = curPageIndex;
                  found = true;
                  break;
               }
               curPageIndex++;
               if (curPageIndex == table.allocated_pages) break;
            }

            fsiOffset++;
         }

         buffer_manager.unfix_page(bf, false);

         if (found) {
            return;
         }
      }

      // there is no other free page with this exact amount of free memory
      free_cache[prevFreeSpace] = invalidPid;
   }
}

void FSISegment::update(uint64_t target_page, uint32_t free_space) {
   // calculate fsi page
   uint64_t tpIndex = target_page & 0x0000FFFFFFFFFFFF;
   uint64_t fsiIndex = tpIndex / (buffer_manager.get_page_size() * 2);
   uint32_t fsiOffset = tpIndex % (buffer_manager.get_page_size() * 2);
   // calculate update byte
   uint8_t efs = encode_free_space(free_space);

   // update
   auto& bf = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ fsiIndex, true);
   auto fsiPtr = reinterpret_cast<uint8_t*>((bf.get_data() + fsiOffset / 2));
   auto fsiVal = *fsiPtr;
   // overwrite upper or lower nibble
   *fsiPtr = fsiOffset % 2 == 0 ? (fsiVal & 0x0F) | (efs << 4) : (fsiVal & 0xF0) | efs;
   buffer_manager.unfix_page(bf, true);

   // update free cache
   update_free_cache(tpIndex, efs);
}

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
   auto ci = encode_free_space(required_space);
   assert(ci >= 0 && ci < 16);

   // search cache
   while (ci < 16) {
      if (free_cache[ci] != invalidPid) {
         // found page with enough free space
         return {free_cache[ci]};
      }
      ci++;
   }

   // no page with enough free space found
   return {};
   /*
   for (uint8_t i = 0; i < 16; ++i) {
      if (decode_free_space(i) >= required_space && free_cache[i] != invalidPid)
         // found page with enough free space
         return {free_cache[i]};
   }

   // no page with enough free space found
   return {};
    */
}
