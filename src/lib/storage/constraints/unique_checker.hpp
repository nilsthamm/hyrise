#pragma once

#include <boost/container/small_vector.hpp>
#include <memory>
#include <functional>
#include <set>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/validate.hpp"
#include "storage/mvcc_data.hpp"
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

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint);
bool check_constraints(std::shared_ptr<const Table> table);
bool check_constraints(std::shared_ptr<const Table> table, const CommitID& snapshot_commit_id, const TransactionID& our_tid);
bool check_constraints(const std::string &table, const CommitID& snapshot_commit_id, const TransactionID& our_tid);

}  // namespace opossum
