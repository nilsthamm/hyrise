#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/constraints/unique_enforcer.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
class ConstraintsTest: public BaseTest {
 protected:
  void SetUp() override {
    column_definitions.emplace_back("unique_column", DataType::Int, false, true);
    column_definitions.emplace_back("other_column", DataType::Int, false, false);

    auto valid_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    auto invalid_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    valid_table->append({1, 1});
    valid_table->append({2, 1});
    valid_table->append({5, 2});
    valid_table->append({0, -1});

    invalid_table->append({1, 1});
    invalid_table->append({2, 1});
    invalid_table->append({5, 2});
    invalid_table->append({1, -1});

    auto& manager = StorageManager::get();
    manager.add_table("validTable", valid_table);
    manager.add_table("invalidTable", invalid_table);
  }

  TableColumnDefinitions column_definitions;
};

TEST_F(ConstraintsTest, TableUniqueValid) {
  const auto table = StorageManager::get().get_table("validTable");
  EXPECT_EQ(does_table_conforms_constraints(table), true);
}

TEST_F(ConstraintsTest, TableUniqueInvalid) {
  const auto table = StorageManager::get().get_table("invalidTable");
  EXPECT_EQ(does_table_conforms_constraints(table), false);
}

}  // namespace opossum
