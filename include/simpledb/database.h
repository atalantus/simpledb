#pragma once

#include "segment.h"
#include "simpledb/buffer_manager.h"
#include "simpledb/schema.h"
#include <memory>

namespace simpledb {

class Database {
   public:
   /// Constructor.
   Database() : buffer_manager(1024, 10) {}

   /// Load a new schema
   void load_new_schema(std::unique_ptr<schema::Schema> schema);
   /// Load schema
   void load_schema(int16_t schema);
   /// Get the currently loaded schema
   schema::Schema& get_schema();
   /// Insert into a table
   void insert(const schema::Table& table, const std::vector<std::string>& data);
   /// Read a tuple by TID from the table
   void read_tuple(const schema::Table& table, TID tid);

   protected:
   /// The buffer manager
   BufferManager buffer_manager;
   /// The segment of the schema
   std::unique_ptr<SchemaSegment> schema_segment;
   /// The segments of the schema's table's slotted pages
   std::unordered_map<int16_t, std::unique_ptr<SPSegment>> slotted_pages;
   /// The segment of the schema's free space inventory
   std::unordered_map<int16_t, std::unique_ptr<FSISegment>> free_space_inventory;
};

}
