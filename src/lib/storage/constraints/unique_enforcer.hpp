#pragma once

#include <memory>
#include <functional>
#include <unordered_map>

#include "storage/table.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

class BaseConstraintEnforcer {
 public:
  BaseConstraintEnforcer(const std::shared_ptr<const Table> table, const ColumnID& column_id) : _table(table), _column_id(column_id) {}
  virtual ~BaseConstraintEnforcer() = default;

  virtual bool conforms_constraint() const = 0;

 protected:
  std::shared_ptr<const Table> _table;
  ColumnID _column_id;
};

template<typename T>
class UniqueConstraintEnforcer : public BaseConstraintEnforcer {
 public:
  UniqueConstraintEnforcer(const std::shared_ptr<const Table> table, const ColumnID& column_id) : BaseConstraintEnforcer(table, column_id) {}
  bool conforms_constraint() const override {
    std::shared_ptr<std::unordered_map<size_t, std::vector<RowID>>> hash_to_column_id = std::make_shared<std::unordered_map<size_t, std::vector<RowID>>>();

    std::hash<T> hasher;
    for(const auto& chunk : _table->chunks()) {
      if(auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(chunk->get_segment(_column_id))) {
        const auto& values = value_segment->values();
        for(ChunkOffset chunk_offset = ChunkOffset{0}; chunk_offset < values.size(); chunk_offset++) {
          const auto hash = hasher(values[chunk_offset]);
          auto iter = hash_to_column_id->find(hash);
          if(iter == hash_to_column_id->end()) {
            hash_to_column_id->insert({hash, {}});
          } else {
            return false;
          }
        }
      } else if(auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(chunk->get_segment(_column_id))) {
        if(dictionary_segment->unique_values_count() != dictionary_segment->size()) {
          return false;
        }
        const auto& values = dictionary_segment->dictionary();
        for(ChunkOffset chunk_offset = ChunkOffset{0}; chunk_offset < values->size(); chunk_offset++) {
          const auto hash = hasher(values->operator[](chunk_offset));
          auto iter = hash_to_column_id->find(hash);
          if(iter == hash_to_column_id->end()) {
            hash_to_column_id->insert({hash, {}});
          } else {
            return false;
          }
        }
      } else if(auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk->get_segment(_column_id))) {
        // TOOD: Maybe iterate over referenced table directly and check if RowID is in pos_list, with appropriate structure (pos_list to set of RowIDs and Chunks)


      } else {
        Fail("Unkown column type");
      }
    }
    return true;
  }
 
 protected:
  // void add_hash(const T& value, std::shared_ptr<const std::hash<T>> hasher, std::shared_ptr<std::unordered_map<size_t, std::vector<RowID>>> hash_map) {

  // }
};


bool does_table_conforms_constraints(std::shared_ptr<const Table> table) {
  auto conforms = true;
  for(const auto& column_id : table->get_unique_columns()) {
    const auto contraint_enforcer = make_shared_by_data_type<BaseConstraintEnforcer, UniqueConstraintEnforcer>(table->column_data_type(column_id), table, column_id);
    conforms &= contraint_enforcer->conforms_constraint();
  }
  return conforms;
}
}  // namespace opossum
