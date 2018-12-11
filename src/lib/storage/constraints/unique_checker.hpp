#pragma once

#include <memory>
#include <functional>
#include <set>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "storage/segment_accessor.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

class BaseConstraintChecker {
 public:
  BaseConstraintChecker(const std::shared_ptr<const Table> table, const ColumnID& column_id) : _table(table), _column_id(column_id) {}
  virtual ~BaseConstraintChecker() = default;

  virtual bool check() const = 0;

 protected:
  std::shared_ptr<const Table> _table;
  ColumnID _column_id;
};

template<typename T>
class UniqueConstraintChecker : public BaseConstraintChecker {
 public:
  UniqueConstraintChecker(
    const std::shared_ptr<const Table> table,
    const ColumnID& column_id) : BaseConstraintChecker(table, column_id) {}
  bool check() const override {
    std::unordered_set<T> unique_values;

    for (const auto& chunk : _table->chunks()) {
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


// New ConstraintChecker that should use vectors of AllTypeVariants to allow concatenated constraints.
class ConcatenatedConstraintChecker {
 public:
  ~ConcatenatedConstraintChecker() = default;

  ConcatenatedConstraintChecker(
    const std::shared_ptr<const Table> table,
    const TableConstraintDefinition& constraint) : _table(table), _constraint(constraint) {}
  bool check() const {
    std::set<std::vector<AllTypeVariant>> unique_values;

    for (const auto& chunk : _table->chunks()) {
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        auto row = std::vector<AllTypeVariant>();
        row.reserve(_constraint.columns.size());
        for (const auto& column_id : _constraint.columns) {
          const auto& segment = chunk->get_segment(column_id);
          // Todo(anybody) Handle NULL values properly.
          row.push_back(segment->operator[](chunk_offset));
        }

        // if (!value.has_value()) {
        //   continue;
        // }
        if (unique_values.count(row)) {
          return false;
        }
        unique_values.insert(row);
      }
    }
    return true;
  }

 protected:
  std::shared_ptr<const Table> _table;
  TableConstraintDefinition _constraint;
};


bool check_constraints(std::shared_ptr<const Table> table) {
  for (const auto& constraint : *(table->get_unique_constraints())) {
    // const auto constraint_checker = make_shared_by_data_type<BaseConstraintChecker, UniqueConstraintChecker>(
    //   table->column_data_type(constraint.columns[0]), table, constraint.columns[0]);
    const auto constraint_checker = std::make_shared<ConcatenatedConstraintChecker>(table, constraint);
    if (!constraint_checker->check()) {
      return false;
    }
  }
  return true;
}
}  // namespace opossum
