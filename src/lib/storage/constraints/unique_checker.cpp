#include "storage/constraints/unique_checker.hpp"

namespace opossum {

bool check_constraint(std::shared_ptr<const Table> table, const TableConstraintDefinition& constraint) {
  const auto constraint_checker = std::make_shared<ConcatenatedConstraintChecker>(table, constraint);
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

}

