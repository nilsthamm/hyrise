#pragma once

#include <unordered_map>

#include "types.hpp"

namespace opossum {

struct TableConstraintDefinition final {
  TableConstraintDefinition() = default;

  bool operator==(const TableConstraintDefinition& rhs) const;

  std::vector<std::vector<ColumnID>> unique_concatenated_columns;
};

// TOOD change value type to vector so that multiple constraints per column are possible
using TableConstraintDefinitions = std::unordered_map<ColumnID, TableConstraintDefinition>;

} // namespace opossum
