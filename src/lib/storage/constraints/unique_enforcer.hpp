#pragma once

#include <memory>
#include <functional>
#include <unordered_set>

#include "storage/table.hpp"
#include "storage/segment_accessor.hpp"
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
    std::unordered_set<T> unique_values;

    for(const auto& chunk : _table->chunks()) {
      const auto& segment = chunk->get_segment(_column_id);
      auto segment_accessor = create_segment_accessor<T>(segment);

      for (ChunkOffset chunk_offset = 0; chunk_offset < segment->size(); chunk_offset++) {
        const std::optional<T>& value = segment_accessor->access(chunk_offset);
        if (!value.has_value()) {
          continue;
        }
        if (unique_values.count(value.value())) {
          return false;
        }
        unique_values.insert(value.value());
      }
    }
    return true;
  }
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
