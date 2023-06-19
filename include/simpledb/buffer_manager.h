#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "simpledb/config.h"
#include "simpledb/file.h"

namespace simpledb {

using Latch = std::shared_mutex;
using ExclusiveLatch = std::unique_lock<Latch>;
using SharedLatch = std::shared_lock<Latch>;

enum class PageState : uint8_t {
   IN_FIFO,
   IN_LRU,
   NOT_LOADED,
   LOADING
};

class BufferFrame {
   private:
   friend class BufferManager;

   uint64_t pid;
   PageState pageState;
   Latch pageLatch;
   std::mutex loadingLatch;
   bool isDirty;
   char* data;

   public:
   explicit BufferFrame(uint64_t pid) : pid{pid}, pageState{PageState::NOT_LOADED}, isDirty{false}, data{nullptr} {}

   BufferFrame(const BufferFrame& frame) = delete;
   BufferFrame& operator=(const BufferFrame& frame) = delete;
   BufferFrame(BufferFrame&& frame) noexcept = delete;
   BufferFrame& operator=(BufferFrame&& frame) noexcept = delete;

   /// Returns a pointer to this page's data.
   char* get_data();
};

class buffer_full_error
   : public std::exception {
   public:
   [[nodiscard]] const char* what() const noexcept override {
      return "buffer is full";
   }
};

class BufferManager {
   private:
   // parameters
   const size_t pageSize;
   const size_t pageCount;

   // data structures
   std::unique_ptr<std::array<std::pair<std::unique_ptr<File>, Latch>, 65536>> segments;
   std::unordered_map<uint64_t, BufferFrame> pageTable;
   std::vector<BufferFrame*> fifoList; // TODO: Use std::list or std::forward_list
   std::vector<BufferFrame*> lruList;

   // latches
   mutable Latch pageTableLatch;
   mutable Latch fifoListLatch;
   mutable Latch lruListLatch;

   public:
   BufferManager(const BufferManager&) = delete;
   BufferManager(BufferManager&&) = delete;
   BufferManager& operator=(const BufferManager&) = delete;
   BufferManager& operator=(BufferManager&&) = delete;
   /// Constructor.
   /// @param[in] page_size  Size in bytes that all pages will have.
   /// @param[in] page_count Maximum number of pages that should reside in
   //                        memory at the same time.
   BufferManager(size_t page_size, size_t page_count);

   /// Destructor. Writes all dirty pages to disk.
   ~BufferManager();

   /// Get the BufferFrame for a given page id.
   /// thread-safe.
   BufferFrame* getBufferFrame(uint64_t pageId);

   std::unique_ptr<char[]> getSegmentData(uint64_t pid);

   /// Load the page for a given BufferFrame into memory.
   /// Returns true on success.
   /// thread-safe.
   bool loadPage(BufferFrame& frame);

   void updateLru(BufferFrame* frame, ExclusiveLatch& exclLruListLatch);

   /// Insert the given BufferFrame into the fifo list.
   /// Returns true on success.
   /// thread-safe.
   bool insertBufferFrame(BufferFrame& frame);

   /// Find the first BufferFrame in the given list that is not locked in any mode, locks it
   /// and return its index.
   /// NOT thread-safe.
   static long lockEvictableFrame(std::vector<BufferFrame*>& frameList, ExclusiveLatch& frameListLatch);

   /// Flush a BufferFrame's page to disk.
   /// NOT thread-safe.
   void flushPage(BufferFrame& frame);

   /// Returns a reference to a `BufferFrame` object for a given page id. When
   /// the page is not loaded into memory, it is read from disk. Otherwise the
   /// loaded page is used.
   /// When the page cannot be loaded because the buffer is full, throws the
   /// exception `buffer_full_error`.
   /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
   /// `unfix_page()`.
   /// @param[in] page_id   Page id of the page that should be loaded.
   /// @param[in] exclusive If `exclusive` is true, the page is locked
   ///                      exclusively. Otherwise it is locked
   ///                      non-exclusively (shared).
   BufferFrame& fix_page(uint64_t page_id, bool exclusive);

   /// Takes a `BufferFrame` reference that was returned by an earlier call to
   /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
   /// written back to disk eventually.
   void unfix_page(BufferFrame& page, bool is_dirty);

   /// Returns the page ids of all pages (fixed and unfixed) that are in the
   /// FIFO list in FIFO order.
   /// Is not thread-safe.
   [[nodiscard]] std::vector<uint64_t> get_fifo_list() const;

   /// Returns the page ids of all pages (fixed and unfixed) that are in the
   /// LRU list in LRU order.
   /// Is not thread-safe.
   [[nodiscard]] std::vector<uint64_t> get_lru_list() const;

   // TODO: Remove and refactor
   static uint32_t get_page_size() { return PAGE_SIZE; }

   /// Returns the segment id for a given page id which is contained in the 16
   /// most significant bits of the page id.
   static constexpr uint16_t get_segment_id(uint64_t page_id) {
      return page_id >> 48;
   }

   /// Returns the page id within its segment for a given page id. This
   /// corresponds to the 48 least significant bits of the page id.
   static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
      return page_id & ((1ull << 48) - 1);
   }
};

}
