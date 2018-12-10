#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

struct TableConstraintDefinition final {
  TableConstraintDefinition() = default;

  bool operator==(const TableConstraintDefinition& rhs) const;

  std::vector<ColumnID> columns;
  bool is_primary_key;
};

// TOOD change value type to vector so that multiple constraints per column are possible
using TableConstraintDefinitions = std::vector<TableConstraintDefinition>;

}  // namespace opossum
