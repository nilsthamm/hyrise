#pragma once

#include <memory>

#include "storage/table.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

class BaseConstraintEnforcer {
  public:
    bool conforms_contraint() const;

  protected:
    std::shared_ptr<const Table> _table;
    ColumnID _column_id;
}


bool does_table_conforms_constraints(std::shared_ptr<const Table> table) {
  auto conforms = true;

  return conforms;
}
}  // namespace opossum
