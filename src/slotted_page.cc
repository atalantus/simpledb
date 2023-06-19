#include "simpledb/slotted_page.h"
#include <cstring>

using simpledb::SlottedPage;

SlottedPage::Header::Header(uint32_t page_size)
   : slot_count(0),
     first_free_slot(0),
     data_start(page_size),
     free_space(page_size - sizeof(Header)) {}

SlottedPage::SlottedPage(uint32_t page_size)
   : header(page_size) {
   std::memset(get_data() + sizeof(SlottedPage), 0x00, page_size - sizeof(SlottedPage));
}

std::byte* SlottedPage::get_data() {
   return reinterpret_cast<std::byte*>(this);
}

const std::byte* SlottedPage::get_data() const {
   return reinterpret_cast<const std::byte*>(this);
}

SlottedPage::Slot* SlottedPage::get_slots() {
   return reinterpret_cast<SlottedPage::Slot*>(get_data() + sizeof(SlottedPage));
}

const SlottedPage::Slot* SlottedPage::get_slots() const {
   return reinterpret_cast<const SlottedPage::Slot*>(get_data() + sizeof(SlottedPage));
}

SlottedPage::Slot& SlottedPage::get_slot(uint16_t slot_id) {
   assert(slot_id < header.slot_count);
   return get_slots()[slot_id];
}

const SlottedPage::Slot& SlottedPage::get_slot(uint16_t slot_id) const {
   assert(slot_id < header.slot_count);
   return get_slots()[slot_id];
}

uint32_t SlottedPage::get_fragmented_free_space() const {
   return header.data_start - sizeof(Header) - header.slot_count * sizeof(Slot);
}

uint16_t SlottedPage::allocate(uint32_t data_size, uint32_t page_size, bool is_redirect_target) {
   assert(header.free_space >= data_size + (header.first_free_slot < header.slot_count ? 0 : sizeof(Slot)));

   if (get_fragmented_free_space() <= data_size + (header.first_free_slot < header.slot_count ? 0 : sizeof(Slot))) {
      // need to compactify before
      compactify(page_size);
   }

   uint16_t slotId;
   if (header.first_free_slot >= header.slot_count) {
      // allocate new slot
      slotId = header.slot_count++;
      header.free_space -= sizeof(Slot);
   } else {
      // reuse free slot
      slotId = header.first_free_slot;
   }

   // allocate data
   header.data_start -= data_size;
   header.free_space -= data_size;
   // set slot
   get_slots()[slotId].set_slot(header.data_start, data_size, is_redirect_target);

   // find next free slot
   const auto slots = get_slots();
   for (; header.first_free_slot < header.slot_count; ++header.first_free_slot) {
      if (slots[header.first_free_slot].is_empty()) break;
   }

   return slotId;
}

void SlottedPage::relocate(uint16_t slot_id, uint32_t data_size, uint32_t page_size) {
   auto& slot = get_slot(slot_id);

   assert(!slot.is_redirect() && !slot.is_empty() && (data_size <= slot.get_size() || header.free_space >= data_size - slot.get_size()));

   if (data_size <= slot.get_size()) {
      // just resize slot
      header.free_space += slot.get_size() - data_size;
      slot.set_slot(slot.get_offset(), data_size, slot.is_redirect_target());
   } else if (get_fragmented_free_space() >= data_size) {
      // just reallocate slot
      header.data_start -= data_size;
      header.free_space += slot.get_size();
      header.free_space -= data_size;
      // copy data
      memcpy(get_data() + header.data_start, get_data() + slot.get_offset(), slot.get_size());
      // update slot
      slot.set_slot(header.data_start, data_size, slot.is_redirect_target());
   } else {
      // not enough space -> increase slot size and compactify
      slot.set_slot(slot.get_offset(), data_size, slot.is_redirect_target());
      compactify(page_size);
   }
}

void SlottedPage::erase(uint16_t slot_id) {
   auto& slot = get_slot(slot_id);

   header.free_space += slot.get_size();

   if (slot_id < header.first_free_slot)
      header.first_free_slot = slot_id;

   // if slot's data is first we optimize
   if (slot.get_offset() == header.data_start)
      header.data_start += slot.get_size();

   // clear slot
   slot.clear();

   // if it's the last slot we optimize
   if (slot_id + 1 == header.slot_count) {
      for (; header.slot_count > 0; --header.slot_count, header.free_space += sizeof(Slot))
         if (!get_slot(header.slot_count - 1).is_empty())
            break;
   }
}

void SlottedPage::compactify(uint32_t page_size) {
   char* tempData = new char[page_size];
   auto& tempPage = *reinterpret_cast<SlottedPage*>(tempData);

   tempPage.header.data_start = page_size;

   for (uint16_t s = 0; s < header.slot_count; ++s) {
      auto slot = get_slot(s);
      auto& newSlot = *reinterpret_cast<Slot*>(tempData + sizeof(Header) + s * sizeof(Slot));

      // copy slot
      memcpy(&newSlot, &slot, sizeof(Slot));

      if (slot.is_empty() || slot.is_redirect())
         continue;

      // copy data
      tempPage.header.data_start -= slot.get_size();
      auto dataSize = std::min(page_size - slot.get_offset(), slot.get_size());
      memcpy(tempData + tempPage.header.data_start, get_data() + slot.get_offset(), dataSize);

      // update slot
      newSlot.set_slot(tempPage.header.data_start, slot.get_size(), slot.is_redirect_target());
   }

   tempPage.header.slot_count = header.slot_count;
   tempPage.header.first_free_slot = header.first_free_slot;
   tempPage.header.free_space = tempPage.get_fragmented_free_space();

   // copy page back
   memcpy(this, tempData, page_size);
   delete[] tempData;
}
