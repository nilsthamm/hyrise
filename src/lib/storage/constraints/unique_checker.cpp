#include <string>

#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint) {
  return false;
}

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint, const CommitID& last_commit_id, const TransactionID& our_tid) {
  const auto constraint_checker = std::make_shared<ConcatenatedConstraintChecker>(table, constraint, last_commit_id, our_tid);
  return constraint_checker->check();
}

bool check_constraints(std::shared_ptr<const Table> table) {
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!check_constraint(table, constraint)) {
      return false;
    }
  }
  return true;
}

bool check_constraints(const std::string &table_name, const CommitID& last_commit_id, const TransactionID& our_tid) {
  std::cout << "Table name: " << table_name << std::endl;
  auto const table = StorageManager::get().get_table(table_name);
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!check_constraint(table, constraint, last_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

}

