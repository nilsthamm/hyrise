#include <string>

#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint) {
  return false;
}

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  if (constraint.is_primary_key) {
    for (const auto& column_id : constraint.columns) {
      if (table->column_is_nullable(column_id)) {
        return false;
      }
    }
  }

  std::set<boost::container::small_vector<AllTypeVariant, 3>> unique_values;

  for (const auto& chunk : table->chunks()) {
    const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    const auto& segments = chunk->segments();
    for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
      const auto row_tid = mvcc_data->tids[chunk_offset].load();
      const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
      const auto end_cid = mvcc_data->end_cids[chunk_offset];
      auto row = boost::container::small_vector<AllTypeVariant, 3>();
      row.reserve(constraint.columns.size());
      bool row_contains_null = false;

      if (!Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
        continue;
      }
      for (const auto& column_id : constraint.columns) {
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

bool check_constraints(std::shared_ptr<const Table> table, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!check_constraint(table, constraint, snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

bool check_constraints(std::shared_ptr<const Table> table) {
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!check_constraint(table, constraint)) {
      return false;
    }
  }
  return true;
}

bool check_constraints(const std::string &table_name, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!check_constraint(table, constraint, snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

}

