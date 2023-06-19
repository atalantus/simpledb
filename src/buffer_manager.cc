#include "simpledb/buffer_manager.h"
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

namespace simpledb {

char* BufferFrame::get_data() {
   assert(pageState == PageState::IN_FIFO || pageState == PageState::IN_LRU);
   return data;
}

BufferManager::BufferManager(size_t page_size, size_t page_count) : pageSize{page_size}, pageCount{page_count} {
   assert(page_size == PAGE_SIZE);
   segments = std::make_unique<std::array<std::pair<std::unique_ptr<File>, Latch>, 65536>>();

   fifoList.reserve(page_count);
   lruList.reserve(page_count);
}

BufferManager::~BufferManager() {
   // write out dirty pages and free data memory

   fifoListLatch.lock();
   for (auto bf : fifoList) {
      bf->pageLatch.lock();
      assert(bf->pageState == PageState::IN_FIFO);

      if (bf->isDirty)
         flushPage(*bf);

      assert(bf->data);
      free(bf->data);

      bf->pageLatch.unlock();
   }
   fifoListLatch.unlock();

   lruListLatch.lock();
   for (auto bf : lruList) {
      bf->pageLatch.lock();
      assert(bf->pageState == PageState::IN_LRU);

      if (bf->isDirty)
         flushPage(*bf);

      assert(bf->data);
      free(bf->data);

      bf->pageLatch.unlock();
   }
   lruListLatch.unlock();
}

BufferFrame* BufferManager::getBufferFrame(uint64_t pageId) {
   BufferFrame* frame;

   pageTableLatch.lock_shared();

   // check if page is already contained in the page table
   if (pageTable.contains(pageId)) {
      frame = &pageTable.at(pageId);

      pageTableLatch.unlock_shared();
   } else {
      // we will need an exclusive lock on the hashtable
      pageTableLatch.unlock_shared();
      pageTableLatch.lock();

      if (pageTable.contains(pageId)) {
         // someone inserted this page in the meantime
         frame = &pageTable.at(pageId);
      } else {
         // insert new buffer frame
         auto p = pageTable.emplace(pageId, pageId);

         if (!p.second) {
            throw std::runtime_error("Error while inserting new buffer frame into page table");
         }

         frame = &p.first->second;
      }

      pageTableLatch.unlock();
   }

   return frame;
}

std::unique_ptr<char[]> BufferManager::getSegmentData(uint64_t pid) {
   auto segId = get_segment_id(pid);
   auto segIdString = std::to_string(segId);
   auto segPageId = get_segment_page_id(pid);

   auto minSize = segPageId * pageSize + pageSize;
   auto& seg = (*segments)[segId];

   // check if segment file does not exist yet
   if (!seg.first) {
      seg.second.lock();

      // has it been created in the meantime?
      if (!seg.first) {
         seg.first = File::open_file(segIdString.c_str(), File::WRITE);
      }

      seg.second.unlock();
   }

   // check if segment file is big enough
   if (seg.first->size() < minSize) {
      seg.second.lock();

      // has it been resized in the meantime
      if (seg.first->size() < minSize) {
         seg.first->resize(minSize);
      }

      seg.second.unlock();
   }

   // read page data from segment file
   seg.second.lock_shared();
   auto data = seg.first->read_block(segPageId * pageSize, pageSize);
   seg.second.unlock_shared();

   return data;
}

bool BufferManager::loadPage(simpledb::BufferFrame& frame) {
   frame.loadingLatch.lock();

   if (frame.pageState == PageState::LOADING) {
      throw std::runtime_error("Frame in invalid state. We hold the loading latch but page is in loading?");
   }

   if (frame.pageState == PageState::IN_FIFO || frame.pageState == PageState::IN_LRU) {
      // someone else loaded before us
      frame.loadingLatch.unlock();
      return true;
   }

   // load the page
   assert(frame.pageState == PageState::NOT_LOADED);
   frame.pageState = PageState::LOADING;

   // try insert the buffer frame into the fifo list
   if (!insertBufferFrame(frame)) {
      frame.pageState = PageState::NOT_LOADED;
      frame.loadingLatch.unlock();
      return false;
   }

   // load data
   auto data = getSegmentData(frame.pid);
   frame.data = (char*) malloc(pageSize);
   assert(frame.data);
   memcpy(frame.data, data.get(), pageSize);

   // done loading
   frame.pageState = PageState::IN_FIFO;
   frame.loadingLatch.unlock();
   return true;
}

bool BufferManager::insertBufferFrame(simpledb::BufferFrame& frame) {
   ExclusiveLatch exclFifoLatch(fifoListLatch);

   // check if we still have free space
   lruListLatch.lock_shared();
   if (fifoList.size() + lruList.size() < pageCount) {
      // just insert at the back of the fifo list
      fifoList.push_back(&frame);

      // done
      lruListLatch.unlock_shared();
      exclFifoLatch.unlock();
      return true;
   }
   lruListLatch.unlock_shared();

   // find a free spot in fifo list
   auto i = lockEvictableFrame(fifoList, exclFifoLatch);
   if (i != -1) {
      auto bf = fifoList[i];
      assert(bf->pageState == PageState::IN_FIFO);

      // evict old frame
      fifoList.erase(fifoList.begin() + i);

      // insert new frame
      fifoList.push_back(&frame);

      exclFifoLatch.unlock();

      if (bf->isDirty) {
         // frame is dirty. flush it to disk
         flushPage(*bf);
      }
      assert(bf->pageState == PageState::IN_FIFO);
      bf->pageState = PageState::NOT_LOADED;
      assert(bf->data);
      free(bf->data);

      bf->pageLatch.unlock();

      return true;
   }

   // find a free spot in the lru list
   ExclusiveLatch exclLruLatch(lruListLatch);
   i = lockEvictableFrame(lruList, exclLruLatch);
   if (i != -1) {
      auto bf = lruList[i];
      assert(bf->pageState == PageState::IN_LRU);

      // evict old frame
      lruList.erase(lruList.begin() + i);

      exclLruLatch.unlock();

      // insert new frame
      fifoList.push_back(&frame);

      exclFifoLatch.unlock();

      if (bf->isDirty) {
         // frame is dirty. flush it to disk
         flushPage(*bf);
      }
      assert(bf->pageState == PageState::IN_LRU);
      bf->pageState = PageState::NOT_LOADED;
      assert(bf->data);
      free(bf->data);

      bf->pageLatch.unlock();

      return true;
   }

   // couldn't find a free spot anywhere :(
   exclLruLatch.unlock();
   exclFifoLatch.unlock();
   return false;
}

long BufferManager::lockEvictableFrame(std::vector<BufferFrame*>& frameList, ExclusiveLatch&) {
   for (size_t i = 0; i < frameList.size(); ++i) {
      if (frameList[i]->pageLatch.try_lock())
         return static_cast<long>(i);
   }
   return -1;
}

void BufferManager::updateLru(simpledb::BufferFrame* frame, simpledb::ExclusiveLatch&) {
   uint64_t rpid = 0;
   bool erased = false;

   // find in lru list
   // TODO: Iterate full list????
   for (auto it = lruList.begin(); it != lruList.end(); ++it) {
      if ((*it) == frame) {
         erased = true;
         rpid = (*it)->pid;
         lruList.erase(it);
         break;
      }
   }

   assert(erased);
   assert(frame->pid == rpid);

   // reinsert in lru list
   lruList.push_back(frame);
}

void BufferManager::flushPage(simpledb::BufferFrame& frame) {
   auto segId = get_segment_id(frame.pid);
   auto segPageId = get_segment_page_id(frame.pid);

   auto& seg = (*segments)[segId];

   seg.second.lock_shared();

   // write changes to disk
   auto data = frame.get_data();
   seg.first->write_block(data, segPageId * pageSize, pageSize);

   frame.isDirty = false;
   seg.second.unlock_shared();
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
   BufferFrame* frame = getBufferFrame(page_id);

   // acquire page latch in given mode
   if (exclusive) {
      frame->pageLatch.lock();
   } else {
      frame->pageLatch.lock_shared();
   }

   // check if page is already in memory and if not try loading it in
   switch (frame->pageState) {
      case PageState::IN_FIFO: {
         // move from fifo to lru list
         ExclusiveLatch exclFifoLatch(fifoListLatch);
         ExclusiveLatch exclLruLatch(lruListLatch);

         // could have been moved to LRU in the meantime
         if (frame->pageState == PageState::IN_LRU) {
            updateLru(frame, exclLruLatch);
            break;
         }

         assert(frame->pageState == PageState::IN_FIFO);

         uint64_t rpid = 0;
         bool erased = false;

         // find in fifo list
         // TODO: Iterate full list????
         for (auto it = fifoList.begin(); it != fifoList.end(); ++it) {
            if ((*it) == frame) {
               erased = true;
               rpid = (*it)->pid;
               fifoList.erase(it);
               break;
            }
         }

         assert(erased);
         assert(rpid == frame->pid);

         // insert in lru list
         lruList.push_back(frame);
         frame->pageState = PageState::IN_LRU;
         break;
      }
      case PageState::IN_LRU: {
         // move to the back of lru
         ExclusiveLatch exclLruLatch(lruListLatch);

         assert(frame->pageState == PageState::IN_LRU);

         updateLru(frame, exclLruLatch);
         break;
      }
      case PageState::NOT_LOADED:
         // try load page into memory
         if (!loadPage(*frame)) {
            frame->pageLatch.unlock();
            throw buffer_full_error();
         }
         break;
      case PageState::LOADING:
         // wait for page to load
         frame->loadingLatch.lock();
         frame->loadingLatch.unlock();
         if (frame->pageState != PageState::IN_FIFO && frame->pageState != PageState::IN_LRU) {
            //std::cerr << "waited for failed page load" << std::endl;
            frame->pageLatch.unlock();
            throw buffer_full_error();
         }
         break;
   }

   return *frame;
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
   // the premise is that unfix_page is never called by a thread that fixed it in shared mode
   // with the is_dirty flag set to true, as this wouldn't make any sense
   page.isDirty = page.isDirty || is_dirty;
   page.pageLatch.unlock();
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
   SharedLatch latch(fifoListLatch);
   std::vector<uint64_t> v;
   v.reserve(fifoList.size());
   for (auto bf : fifoList) {
      v.push_back(bf->pid);
   }
   return v;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
   SharedLatch latch(lruListLatch);
   std::vector<uint64_t> v;
   v.reserve(lruList.size());
   for (auto bf : lruList) {
      v.push_back(bf->pid);
   }
   return v;
}

}
