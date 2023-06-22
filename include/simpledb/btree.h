#pragma once

#include "simpledb/binary_search.h"
#include "simpledb/buffer_manager.h"
#include "simpledb/segment.h"
#include <atomic>
#include <cassert>
#include <cstring>
#include <mutex>

namespace simpledb {

template <typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
   struct Node {
      /// The level in the tree.
      uint16_t level;
      /// The number of children.
      uint16_t count;

      // Constructor
      Node(uint16_t level, uint16_t count)
         : level(level), count(count) {}

      /// Is the node a leaf node?
      [[nodiscard]] bool is_leaf() const { return level == 0; }
   };

   struct InnerNode : public Node {
      /// The capacity of children of a node.
      static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(uint64_t));

      /// The keys.
      KeyT keys[kCapacity];
      /// The children.
      uint64_t children[kCapacity];

      /// Constructor.
      InnerNode() : Node(0, 0) {}

      /// Returns if there is enough space left to fit another entry.
      [[nodiscard]] bool has_space() const { return this->count < kCapacity; };

      /// Get the index of the first key that is not less than than a provided key.
      /// @param[in] key          The key that should be inserted.
      std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
         if (this->count == 0)
            return {0, false};
         auto cmp = ComparatorT{};
         auto i = lower_bound_branchless(&keys[0], &keys[this->count - 1], key, cmp);
         return {i, i < this->count - 1UL && keys[i] == key};
      }

      /// Insert a key.
      /// @param[in] key          The key that should be inserted.
      /// @param[in] split_page   The child that should be inserted to the right.
      void insert_split(const KeyT& key, uint64_t split_page) {
         assert(has_space());

         auto pos = lower_bound(key);

         assert(!pos.second && "Split key already exists?");

         /*
         if (pos.second) {
            // key already exists
            children[pos.first] = split_page;
            return;
         }
          */

         if (pos.first + 1 < this->count) {
            // move keys
            memmove(&keys[pos.first + 1], &keys[pos.first], (this->count - pos.first - 1) * sizeof(KeyT));
            // move children
            memmove(&children[pos.first + 1], &children[pos.first], (this->count - pos.first) * sizeof(uint64_t));
         }

         // insert split key and child
         keys[pos.first] = key;
         children[pos.first + 1] = split_page;

         ++this->count;
      }

      /// Split the node.
      /// @param[in] buffer       The buffer for the new page.
      /// @return                 The separator key.
      KeyT split(std::byte* buffer) {
         auto other = reinterpret_cast<InnerNode*>(buffer);
         other->level = this->level;
         other->count = this->count / 2;
         auto newCount = this->count - other->count;

         for (size_t i = newCount; i < this->count; ++i) {
            other->keys[i - newCount] = keys[i];
            other->children[i - newCount] = children[i];
         }

         this->count = newCount;
         return keys[newCount - 1];
      }

      /// Returns the keys.
      std::vector<KeyT> get_key_vector() {
         std::vector<KeyT> vec;

         for (size_t i = 0; i < this->count - 1; ++i) {
            vec.emplace_back(keys[i]);
         }

         return vec;
      }

      /// Returns the children.
      std::vector<KeyT> get_children_vector() {
         std::vector<KeyT> vec;

         for (size_t i = 0; i < this->count; ++i) {
            vec.emplace_back(children[i]);
         }

         return vec;
      }
   };

   struct LeafNode : public Node {
      /// The capacity of a node.
      static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));

      /// The keys.
      KeyT keys[kCapacity];
      /// The values.
      ValueT values[kCapacity];

      /// Constructor.
      LeafNode() : Node(0, 0) {}

      /// Returns if there is enough space left to fit another entry.
      [[nodiscard]] bool has_space() const { return this->count < kCapacity; };

      /// Get the index of the first key that is not less than than a provided key.
      std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
         if (this->count == 0)
            return {0, false};
         auto cmp = ComparatorT{};
         auto i = lower_bound_branchless(&keys[0], &keys[this->count], key, cmp);
         return {i, i < this->count && keys[i] == key};
      }

      /// Insert a key.
      /// @param[in] key          The key that should be inserted.
      /// @param[in] value        The value that should be inserted.
      void insert(const KeyT& key, const ValueT& value) {
         assert(has_space());

         auto pos = lower_bound(key);

         if (pos.second) {
            // key already exists
            values[pos.first] = value;
            return;
         }

         // move keys
         memmove(&keys[pos.first + 1], &keys[pos.first], (this->count - pos.first) * sizeof(KeyT));
         // move values
         memmove(&values[pos.first + 1], &values[pos.first], (this->count - pos.first) * sizeof(ValueT));

         // insert key, value
         keys[pos.first] = key;
         values[pos.first] = value;

         ++this->count;
      }

      /// Erase a key.
      /// Returns if the key was erased.
      bool erase(const KeyT& key) {
         if (this->count == 0)
            return false;

         auto pos = lower_bound(key);

         if (!pos.second)
            // key does not exist
            return false;

         assert(ComparatorT{}(keys[pos.first], key) == 0);

         if (this->count > pos.first + 1) {
            // move keys
            memmove(&keys[pos.first], &keys[pos.first + 1], (this->count - (pos.first + 1)) * sizeof(KeyT));
            // move values
            memmove(&values[pos.first], &values[pos.first + 1], (this->count - (pos.first + 1)) * sizeof(ValueT));
         }

         --this->count;

         return true;
      }

      /// Split the node.
      /// @param[in] buffer       The buffer for the new page.
      /// @return                 The separator key.
      KeyT split(std::byte* buffer) {
         auto other = reinterpret_cast<LeafNode*>(buffer);
         other->level = this->level;
         other->count = this->count / 2;
         auto newCount = this->count - other->count;

         for (size_t i = newCount; i < this->count; ++i) {
            other->keys[i - newCount] = keys[i];
            other->values[i - newCount] = values[i];
         }

         this->count = newCount;
         return keys[newCount - 1];
      }

      /// Returns the keys.
      std::vector<KeyT> get_key_vector() {
         std::vector<KeyT> vec;

         for (size_t i = 0; i < this->count; ++i) {
            vec.emplace_back(keys[i]);
         }

         return vec;
      }

      /// Returns the values.
      std::vector<ValueT> get_value_vector() {
         std::vector<KeyT> vec;

         for (size_t i = 0; i < this->count; ++i) {
            vec.emplace_back(values[i]);
         }

         return vec;
      }
   };

   /// The root.
   uint64_t root;

   /// Node count.
   std::atomic<uint64_t> nodeCount = 0;
   /// Tree height.
   std::atomic<uint16_t> treeHeight = 0;

   /// Constructor.
   BTree(uint16_t segment_id, BufferManager& buffer_manager)
      : Segment(segment_id, buffer_manager) {
      auto pid = create_new_node();
      auto& bf = buffer_manager.fix_page(pid, true);
      new (bf.get_data()) LeafNode();

      root = pid;
      treeHeight = 1;

      buffer_manager.unfix_page(bf, true);
   }

   /// Destructor.
   ~BTree() = default;

   /// Creates a new node and returns its PID.
   uint64_t create_new_node() {
      return (static_cast<uint64_t>(segment_id) << 48) ^ nodeCount++;
   }

   /// Grows the root of the tree and returns the new root's buffer frame.
   BufferFrame& grow_root(uint16_t level, KeyT sep_key, uint64_t left_child, uint64_t right_child) {
      auto pid = create_new_node();
      auto& bf = buffer_manager.fix_page(pid, true);
      auto& newRoot = *reinterpret_cast<InnerNode*>(bf.get_data());

      newRoot.level = level;
      newRoot.count = 2;

      newRoot.keys[0] = sep_key;
      newRoot.children[0] = left_child;
      newRoot.children[1] = right_child;

      root = pid;
      ++treeHeight;

      return bf;
   }

   /// Lookup an entry in the tree.
   /// @param[in] key      The key that should be searched.
   /// @return             Whether the key was in the tree.
   std::optional<ValueT> lookup(const KeyT& key) {
   restart:
      BufferFrame* parentFrame = nullptr;
      BufferFrame* currentFrame;

      auto rootPid = root;
      currentFrame = &buffer_manager.fix_page(rootPid, false);

      if (root != rootPid) {
         // root changed -> restart
         buffer_manager.unfix_page(*currentFrame, false);
         goto restart;
      }

      while (!reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf()) {
         auto& innerNode = *reinterpret_cast<InnerNode*>(currentFrame->get_data());

         auto pos = innerNode.lower_bound(key);
         assert(pos.first <= innerNode.count);

         // move down
         if (parentFrame) {
            buffer_manager.unfix_page(*parentFrame, false);
         }
         parentFrame = currentFrame;
         currentFrame = &buffer_manager.fix_page(innerNode.children[pos.first], false);
      }

      assert(reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf());

      auto& leafNode = *reinterpret_cast<LeafNode*>(currentFrame->get_data());

      auto pos = leafNode.lower_bound(key);
      assert(pos.first <= leafNode.count);

      auto val = pos.second ? std::optional<ValueT>(leafNode.values[pos.first]) : std::optional<ValueT>{};

      if (parentFrame) {
         buffer_manager.unfix_page(*parentFrame, false);
      }
      buffer_manager.unfix_page(*currentFrame, false);

      return val;
   }

   /// Erase an entry in the tree.
   /// @param[in] key      The key that should be searched.
   void erase(const KeyT& key) {
   restart:
      BufferFrame* parentFrame = nullptr;
      BufferFrame* currentFrame;

      auto rootPid = root;
      currentFrame = &buffer_manager.fix_page(rootPid, treeHeight == 1);

      if (root != rootPid) {
         // root changed -> restart
         buffer_manager.unfix_page(*currentFrame, false);
         goto restart;
      }

      while (!reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf()) {
         auto& innerNode = *reinterpret_cast<InnerNode*>(currentFrame->get_data());

         auto pos = innerNode.lower_bound(key);
         assert(pos.first <= innerNode.count);

         // move down
         if (parentFrame) {
            buffer_manager.unfix_page(*parentFrame, false);
         }
         parentFrame = currentFrame;
         currentFrame = &buffer_manager.fix_page(innerNode.children[pos.first], innerNode.level == 1);
      }

      assert(reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf());

      auto& leafNode = *reinterpret_cast<LeafNode*>(currentFrame->get_data());

      auto erased = leafNode.erase(key);

      if (parentFrame) {
         buffer_manager.unfix_page(*parentFrame, false);
      }
      buffer_manager.unfix_page(*currentFrame, erased);
   }

   /// Inserts a new entry into the tree.
   /// @param[in] key      The key that should be inserted.
   /// @param[in] value    The value that should be inserted.
   void insert(const KeyT& key, const ValueT& value) {
      bool exclusive = false;

   restart:
      BufferFrame* parentFrame = nullptr;
      uint64_t currentPid;
      BufferFrame* currentFrame;

      currentPid = root;
      currentFrame = &buffer_manager.fix_page(currentPid, exclusive || treeHeight == 1);

      if (root != currentPid) {
         // root changed -> restart
         buffer_manager.unfix_page(*currentFrame, false);
         goto restart;
      }

      while (!reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf()) {
         auto& innerNode = *reinterpret_cast<InnerNode*>(currentFrame->get_data());

         if (!innerNode.has_space()) {
            // we have to split

            if (!exclusive) {
               // we are not exclusive -> restart
               buffer_manager.unfix_page(*currentFrame, false);
               if (parentFrame)
                  buffer_manager.unfix_page(*parentFrame, false);

               exclusive = true;
               goto restart;
            }

            auto rightPid = create_new_node();
            auto& rightBf = buffer_manager.fix_page(rightPid, true);
            auto splitKey = innerNode.split((std::byte*) rightBf.get_data());

            if (parentFrame) {
               // insert split key into parent
               auto& innerParent = *reinterpret_cast<InnerNode*>(parentFrame->get_data());
               innerParent.insert_split(splitKey, rightPid);
            } else {
               // no parent -> grow root
               auto& newRoot = grow_root(innerNode.level + 1, splitKey, currentPid, rightPid);
               parentFrame = &newRoot;
            }

            buffer_manager.unfix_page(rightBf, true);
            buffer_manager.unfix_page(*currentFrame, true);
            buffer_manager.unfix_page(*parentFrame, true);

            // restart again without exclusive
            exclusive = false;
            goto restart;
         }

         auto pos = innerNode.lower_bound(key);
         assert(pos.first <= innerNode.count);

         // move down
         if (parentFrame)
            buffer_manager.unfix_page(*parentFrame, false);
         parentFrame = currentFrame;
         currentFrame = &buffer_manager.fix_page(innerNode.children[pos.first], exclusive || innerNode.level == 1);
         currentPid = innerNode.children[pos.first];
      }

      assert(reinterpret_cast<Node*>(currentFrame->get_data())->is_leaf());

      auto& leafNode = *reinterpret_cast<LeafNode*>(currentFrame->get_data());

      if (!leafNode.has_space()) {
         // we have to split

         if (!exclusive) {
            // we are not exclusive -> restart
            buffer_manager.unfix_page(*currentFrame, false);
            if (parentFrame)
               buffer_manager.unfix_page(*parentFrame, false);

            exclusive = true;
            goto restart;
         }

         auto rightPid = create_new_node();
         auto& rightBf = buffer_manager.fix_page(rightPid, true);
         auto splitKey = leafNode.split((std::byte*) rightBf.get_data());

         if (parentFrame) {
            // insert split key into parent
            auto& innerParent = *reinterpret_cast<InnerNode*>(parentFrame->get_data());
            innerParent.insert_split(splitKey, rightPid);
         } else {
            // no parent -> grow root
            auto& newRoot = grow_root(leafNode.level + 1, splitKey, currentPid, rightPid);
            parentFrame = &newRoot;
         }

         buffer_manager.unfix_page(rightBf, true);
         buffer_manager.unfix_page(*currentFrame, true);
         buffer_manager.unfix_page(*parentFrame, true);

         // restart again without exclusive
         exclusive = false;
         goto restart;
      }

      // insert
      leafNode.insert(key, value);

      buffer_manager.unfix_page(*currentFrame, true);
      if (parentFrame)
         buffer_manager.unfix_page(*parentFrame, false);
   }
};

}
