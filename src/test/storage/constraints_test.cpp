#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
class ConstraintsTest: public BaseTest {
 protected:
  void SetUp() override {
    column_definitions.emplace_back("unique_column", DataType::Int, false);
    column_definitions.emplace_back("other_column", DataType::Int, false);

    auto valid_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    auto invalid_table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    valid_table->add_unique_constraint({ColumnID{0}});
    invalid_table->add_unique_constraint({ColumnID{0}});

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
  EXPECT_TRUE(check_constraints(table));
}

TEST_F(ConstraintsTest, TableUniqueInvalid) {
  const auto table = StorageManager::get().get_table("invalidTable");
  EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsTest, InvalidInsert) {
  // Use private table to not interfere with other test cases
  TableColumnDefinitions valid_table_column_definitions;
  valid_table_column_definitions.emplace_back("unique_column", DataType::Int, false);
  valid_table_column_definitions.emplace_back("other_column", DataType::Int, false);

  auto valid_table = std::make_shared<Table>(valid_table_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  valid_table->add_unique_constraint({ColumnID{0}});

  valid_table->append({1, 1});
  valid_table->append({2, 1});
  valid_table->append({5, 2});
  valid_table->append({0, -1});

  auto& manager = StorageManager::get();
  manager.add_table("InvalidInsert", valid_table);

  // Test if first column is valid before the insert
  EXPECT_TRUE(check_constraints(valid_table));

  // Add the same values again
  auto gt = std::make_shared<GetTable>("validTable");
  gt->execute();


  auto ins = std::make_shared<Insert>("InvalidInsert", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();

  context->commit();

  // Test if enforcer reconizes duplicates
  EXPECT_FALSE(check_constraints(valid_table));
}

TEST_F(ConstraintsTest, ValidInsert) {
  // Use private table to not interfere with other test cases
  TableColumnDefinitions valid_table_column_definitions;
  valid_table_column_definitions.emplace_back("unique_column", DataType::Int, false);
  valid_table_column_definitions.emplace_back("other_column", DataType::Int, false);

  auto valid_table = std::make_shared<Table>(valid_table_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  valid_table->add_unique_constraint({ColumnID{0}});

  valid_table->append({11, 1});
  valid_table->append({21, 1});
  valid_table->append({51, 2});
  valid_table->append({10, -1});

  auto& manager = StorageManager::get();
  manager.add_table("ValidInsert", valid_table);

  // Test if first column is valid before the insert
  EXPECT_TRUE(check_constraints(valid_table));

  // Add the same values again
  auto gt = std::make_shared<GetTable>("validTable");
  gt->execute();


  auto ins = std::make_shared<Insert>("ValidInsert", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);

  ins->execute();

  context->commit();

  // Test if table is still valid
  EXPECT_TRUE(check_constraints(valid_table));
}

}  // namespace opossum
