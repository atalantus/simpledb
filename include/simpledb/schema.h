#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace simpledb {
namespace schema {

struct Type {
   /// Type class
   enum Class : uint8_t {
      kInteger,
      kChar
   };
   /// The type class
   Class tclass;
   /// The type argument (if any)
   uint32_t length;

   /// Get type name
   [[nodiscard]] const char* name() const;

   /// Static methods to construct a type
   static Type Integer();
   static Type Char(unsigned length);
};

struct Column {
   /// Name of the column
   const std::string id;
   /// Type of the column
   const Type type;

   /// Constructor
   explicit Column(std::string id, Type type = Type::Integer())
      : id(std::move(id)), type(type) {}
};

struct Table {
   /// Name of the table
   const std::string id;
   /// Columns
   const std::vector<Column> columns;
   /// Primary key
   const std::vector<std::string> primary_key;
   /// Segment id of the slotted pages
   const uint16_t sp_segment;
   /// Segment id of the free space inventory
   const uint16_t fsi_segment;

   /// Number of allocated slotted pages
   uint64_t allocated_pages;

   /// Constructor
   Table(std::string id, std::vector<Column> columns, std::vector<std::string> primary_key, uint16_t sp_segment, uint16_t fsi_segment, uint64_t allocated_pages = 0)
      : id(std::move(id)), columns(std::move(columns)), primary_key(std::move(primary_key)), sp_segment(sp_segment), fsi_segment(fsi_segment), allocated_pages(allocated_pages) {}
};

struct Schema {
   /// Tables
   std::vector<Table> tables;

   /// Constructor
   explicit Schema(std::vector<Table>&& tables)
      : tables(std::move(tables)) {
   }
};

}
}
