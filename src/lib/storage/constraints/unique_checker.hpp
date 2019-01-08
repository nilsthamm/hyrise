#pragma once

#include <boost/container/small_vector.hpp>
#include <memory>
#include <functional>
#include <set>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
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


// New ConstraintChecker that should use vectors of AllTypeVariants to allow concatenated constraints.
class ConcatenatedConstraintChecker {
 public:
  ~ConcatenatedConstraintChecker() = default;

  ConcatenatedConstraintChecker(
    const std::shared_ptr<const Table> table,
    const TableConstraintDefinition& constraint,
    const CommitID& snapshot_commit_id,
    const TransactionID& our_tid ) : _table(table), _constraint(constraint), _snapshot_commit_id(snapshot_commit_id), _our_tid(our_tid) {}

  bool check() const {
    if (_constraint.is_primary_key) {
      for (const auto& column_id : _constraint.columns) {
        if (_table->column_is_nullable(column_id)) {
          return false;
        }
      }
    }

    std::set<boost::container::small_vector<AllTypeVariant, 3>> unique_values;

    for (const auto& chunk : _table->chunks()) {
      auto mvcc_end_cids = chunk->get_scoped_mvcc_data_lock()->end_cids;
      auto mvcc_begin_cids = chunk->get_scoped_mvcc_data_lock()->begin_cids;
      auto mvcc_tids = chunk->get_scoped_mvcc_data_lock()->begin_cids;

      const auto& segments = chunk->segments();
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        auto row = boost::container::small_vector<AllTypeVariant, 3>();
        row.reserve(_constraint.columns.size());
        bool row_contains_null = false;

        const auto begin_cid = mvcc_begin_cids[chunk_offset];
        const auto end_cid = mvcc_end_cids[chunk_offset];
        const auto row_tid = mvcc_tids[chunk_offset];

        if (_snapshot_commit_id < end_cid && ((_snapshot_commit_id >= begin_cid) != (row_tid == _our_tid))) {
          std::cout << "_snapshot_commit_id: " << _snapshot_commit_id << std::endl;
          std::cout << "MVCC end: " << end_cid << std::endl;
          std::cout << "MVCC begin: " << mvcc_begin_cids[chunk_offset] << std::endl;
          continue;
        }
        // // TODO Explain
        // if (end_cid <= _snapshot_commit_id || begin_cid > _snapshot_commit_id) {
        //   std::cout << "_snapshot_commit_id: " << _snapshot_commit_id << std::endl;
        //   std::cout << "MVCC end: " << end_cid << std::endl;
        //   std::cout << "MVCC begin: " << mvcc_begin_cids[chunk_offset] << std::endl;
        //   continue;
        // }
        for (const auto& column_id : _constraint.columns) {
          const auto& segment = segments[column_id];
          const auto& value = segment->operator[](chunk_offset);
          if (variant_is_null(value)) {
            row_contains_null = true;
          }
          row.emplace_back(value);
        }

        // this handles null values in unique constraints as follows:
        // - if a row doesn't contain any null values, the tuple of the columns must be unique
        // - if a row contains any null values:
        //   - there may be multiple tuples with same values
        //   - because a null value could be any value, these tuples are treated as unique
        //   - so we don't have to put anything into the set of unique values
        if (row_contains_null) {
          continue;
        }
        const auto& [iterator, inserted] = unique_values.insert(row);
        if (!inserted) {
          return false;
        }
      }
    }
    return true;
  }

 protected:
  std::shared_ptr<const Table> _table;
  TableConstraintDefinition _constraint;
  CommitID _snapshot_commit_id;
  TransactionID _our_tid;
};

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint);
bool check_constraints(std::shared_ptr<const Table> table);
bool check_constraints(std::shared_ptr<const Table> table, const CommitID& last_commit_id, const TransactionID& our_tid);
bool check_constraints(const std::string &table, const CommitID& snapshot_commit_id, const TransactionID& our_tid);

}  // namespace opossum
