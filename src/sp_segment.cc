#include "simpledb/segment.h"
#include "simpledb/slotted_page.h"

using simpledb::Segment;
using simpledb::SPSegment;
using simpledb::TID;

SPSegment::SPSegment(uint16_t segment_id, BufferManager& buffer_manager, SchemaSegment& schema, FSISegment& fsi, schema::Table& table)
   : Segment(segment_id, buffer_manager), schema(schema), fsi(fsi), table(table) {
}

TID SPSegment::allocate(uint32_t size, bool is_redirect_target) {
   auto oPid = fsi.find(size + sizeof(SlottedPage::Slot));

   uint64_t pid;
   bool created = false;

   if (oPid.has_value()) {
      pid = (static_cast<uint64_t>(segment_id) << 48) ^ oPid.value();
   } else {
      // create new page
      created = true;
      pid = (static_cast<uint64_t>(segment_id) << 48) ^ table.allocated_pages;
      table.allocated_pages += 1;
   }

   auto bf = &buffer_manager.fix_page(pid, true);
   auto page = reinterpret_cast<SlottedPage*>(bf->get_data());

   if (created) {
      // initialize new page
      new (page) SlottedPage(buffer_manager.get_page_size());
   }

   if (page->get_free_space() < size + sizeof(SlottedPage::Slot)) {
      assert(!created);

      // page actually does NOT have enough space
      buffer_manager.unfix_page(*bf, false);

      // check next bigger encoding
      uint8_t nextEnc = fsi.encode_free_space(size + sizeof(SlottedPage::Slot)) + 1;

      oPid = {};

      if (nextEnc < 16) {
         uint32_t nextSize = fsi.decode_free_space(nextEnc);
         oPid = fsi.find(nextSize);
      }

      created = false;

      if (oPid.has_value()) {
         pid = (static_cast<uint64_t>(segment_id) << 48) ^ oPid.value();
      } else {
         // create new page
         created = true;
         pid = (static_cast<uint64_t>(segment_id) << 48) ^ table.allocated_pages;
         table.allocated_pages += 1;
      }

      bf = &buffer_manager.fix_page(pid, true);
      page = reinterpret_cast<SlottedPage*>(bf->get_data());

      if (created) {
         // initialize new page
         new (page) SlottedPage(buffer_manager.get_page_size());
      }
   }

   // allocate new record
   auto sid = page->allocate(size, buffer_manager.get_page_size(), is_redirect_target);
   buffer_manager.unfix_page(*bf, true);

   // update fsi
   fsi.update(pid, page->get_free_space());

   return {pid, sid};
}

uint32_t SPSegment::read(TID tid, std::byte* record, uint32_t capacity) const {
   auto [bf, page, slot] = get_slot(tid, false);

   assert(!slot.is_redirect_target());

   if (slot.is_empty()) return 0;

   if (!slot.is_redirect()) {
      // read record
      uint32_t size = std::min(capacity, slot.get_size());
      memcpy(record, bf.get_data() + slot.get_offset(), size);
      buffer_manager.unfix_page(bf, false);

      return size;
   } else {
      // follow redirect
      auto rTid = slot.as_redirect_tid();

      buffer_manager.unfix_page(bf, false);

      auto [rBf, rPage, rSlot] = get_slot(rTid, false);

      assert(rSlot.is_redirect_target() && !rSlot.is_empty() && "An empty redirect target doesn't make sense");

      // read record
      uint32_t size = std::min(capacity, rSlot.get_size());
      memcpy(record, rBf.get_data() + rSlot.get_offset(), size);
      buffer_manager.unfix_page(rBf, false);

      return size;
   }
}

uint32_t SPSegment::write(TID tid, std::byte* record, uint32_t record_size) {
   auto [bf, page, slot] = get_slot(tid, true);

   if (!slot.is_redirect()) {
      // write record
      uint32_t size = std::min(record_size, slot.get_size());
      memcpy(bf.get_data() + slot.get_offset(), record, size);

      buffer_manager.unfix_page(bf, true);

      return size;
   } else {
      // follow redirect
      auto rTid = slot.as_redirect_tid();

      buffer_manager.unfix_page(bf, false);

      auto [rBf, rPage, rSlot] = get_slot(rTid, true);

      assert(rSlot.is_redirect_target());

      // write record
      uint32_t size = std::min(record_size, rSlot.get_size());
      memcpy(rBf.get_data() + rSlot.get_offset(), record, size);
      buffer_manager.unfix_page(rBf, true);

      return size;
   }
}

void SPSegment::resize(TID tid, uint32_t new_length) {
   auto [bf, page, slot] = get_slot(tid, true);

   assert(!slot.is_redirect_target());

   if (slot.get_size() == new_length) return;

   if (!slot.is_redirect()) {
      if (new_length < slot.get_size() || page->get_free_space() >= new_length - slot.get_size()) {
         // still fits (compactifies if needed)
         page->relocate(tid.get_slot(), new_length, buffer_manager.get_page_size());
      } else {
         // not enough space -> redirect

         // create new redirect target
         auto newRTid = allocate(new_length, true);
         // copy over data TODO: make this into one operation (allocate and writing)
         write(newRTid, page->get_data() + slot.get_offset(), slot.get_size());

         // update redirect slot
         page->header.free_space += slot.get_size();
         slot.set_redirect_tid(newRTid);
      }

      buffer_manager.unfix_page(bf, true);

      // update fsi
      fsi.update(tid.get_page_id(segment_id), page->get_free_space());
   } else {
      // follow redirect
      auto rTid = slot.as_redirect_tid();
      auto [rBf, rPage, rSlot] = get_slot(rTid, true);

      assert(rSlot.is_redirect_target());

      if (new_length < rSlot.get_size() || rPage->get_free_space() >= new_length - rSlot.get_size()) {
         buffer_manager.unfix_page(bf, false);

         // still fits (compactifies if needed)
         rPage->relocate(rTid.get_slot(), new_length, buffer_manager.get_page_size());

         buffer_manager.unfix_page(rBf, true);
      } else {
         // not enough space -> re-redirect

         // create new redirect target
         auto newRTid = allocate(new_length, true);
         // copy over data TODO: make this into one operation (allocate and writing)
         write(newRTid, rPage->get_data() + rSlot.get_offset(), rSlot.get_size());

         // delete old redirect target
         rPage->erase(rTid.get_slot());
         buffer_manager.unfix_page(rBf, true);

         // update redirect slot
         slot.set_redirect_tid(newRTid);
         buffer_manager.unfix_page(bf, true);
      }

      // update fsi
      fsi.update(rTid.get_page_id(segment_id), rPage->get_free_space());
   }
}

void SPSegment::erase(TID tid) {
   auto [bf, page, slot] = get_slot(tid, true);

   if (!slot.is_redirect()) {
      // erase record
      page->erase(tid.get_slot());
      buffer_manager.unfix_page(bf, true);

      // update fsi
      fsi.update(tid.get_page_id(segment_id), page->get_free_space());
   } else {
      // follow redirect
      auto rTid = slot.as_redirect_tid();

      page->erase(tid.get_slot());
      buffer_manager.unfix_page(bf, true);

      auto [rBf, rPage, rSlot] = get_slot(rTid, true);

      assert(rSlot.is_redirect_target());

      // erase record
      rPage->erase(rTid.get_slot());
      buffer_manager.unfix_page(rBf, true);

      // update fsi
      fsi.update(rTid.get_page_id(segment_id), rPage->get_free_space());
      fsi.update(tid.get_page_id(segment_id), page->get_free_space());
   }
}

std::tuple<simpledb::BufferFrame&, simpledb::SlottedPage*, simpledb::SlottedPage::Slot&> SPSegment::get_slot(simpledb::TID tid, bool exclusive) const {
   auto pid = tid.get_page_id(segment_id);
   auto sid = tid.get_slot();
   auto& bf = buffer_manager.fix_page(pid, exclusive);
   auto page = reinterpret_cast<SlottedPage*>(bf.get_data());
   auto& slot = page->get_slot(sid);
   return {bf, page, slot};
}
