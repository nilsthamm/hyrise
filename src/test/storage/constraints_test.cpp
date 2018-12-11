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
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, true);
    column_definitions.emplace_back("column2", DataType::Int, false);

    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    auto& manager = StorageManager::get();
    manager.add_table("table", table);
  }

  TableColumnDefinitions column_definitions;
};

TEST_F(ConstraintsTest, NullBehaviorForConstraintChecking) {
  // we have to check some ways null values behave for the way we handle concatenated unique keys
  
  AllTypeVariant v1{42};
  AllTypeVariant v2{73};
  AllTypeVariant vnull = NULL_VALUE;

  EXPECT_NE(v1, vnull);
  EXPECT_NE(vnull, vnull);
  EXPECT_FALSE(vnull < vnull);
  EXPECT_FALSE(vnull > vnull);

  std::set<std::vector<AllTypeVariant>> s;
  EXPECT_FALSE(s.count({v1, v2}));
  EXPECT_FALSE(s.count({vnull, v2}));
  EXPECT_FALSE(s.count({vnull, vnull}));

  s.insert({v1, v2});
  EXPECT_TRUE(s.count({v1, v2}));
  EXPECT_FALSE(s.count({vnull, v2}));
  EXPECT_FALSE(s.count({vnull, vnull}));

  // two tuples that are equal and have null values at same places
  // should be treated as equal in a set
  // (contrary to ternary logic where null != null)
  s.insert({vnull, v2});
  EXPECT_TRUE(s.count({vnull, v2}));
  EXPECT_FALSE(s.count({vnull, vnull}));
}

TEST_F(ConstraintsTest, AddUniqueConstraints) {
  auto table = StorageManager::get().get_table("table");
  EXPECT_NO_THROW(table->add_unique_constraint({ColumnID{0}}));
  EXPECT_NO_THROW(table->add_unique_constraint({ColumnID{0}}, true));

  // invalid because invalid column id
  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);
  // invalid because column must be non nullable for primary constrain
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}, true), std::exception);
  // invalid because there is still a nullable column
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);
}

TEST_F(ConstraintsTest, TableSingleUnique) {
  auto table = StorageManager::get().get_table("table");

  table->add_unique_constraint({ColumnID{0}});
  table->append({1, 2, 3});
  table->append({2, 5, 3});
  table->append({3, 3, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({2, 6, 8});
  EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsTest, TableSingleUniqueNullable) {
  auto table = StorageManager::get().get_table("table");

  table->add_unique_constraint({ColumnID{1}});
  table->append({1, 2, 3});
  table->append({2, 5, 3});
  table->append({3, 3, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({3, NULL_VALUE, 4});
  table->append({3, NULL_VALUE, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({3, NULL_VALUE, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({1, 5, 8});
  EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsTest, TableConcatenatedUnique) {
  auto table = StorageManager::get().get_table("table");

  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  table->append({1, 2, 3});
  table->append({2, 5, 3});
  table->append({3, 3, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({1, 5, 3});
  EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsTest, TableConcatenatedUniqueNullable) {
  auto table = StorageManager::get().get_table("table");

  table->add_unique_constraint({ColumnID{1}, ColumnID{2}});
  table->append({1, 2, 3});
  table->append({2, 5, 3});
  table->append({3, 3, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({3, NULL_VALUE, 3});
  table->append({2, NULL_VALUE, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({3, NULL_VALUE, 8});
  EXPECT_TRUE(check_constraints(table));

  table->append({3, 5, 3});
  EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsTest, ValidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  table->add_unique_constraint({ColumnID{0}});
  table->append({1, 1, 3});
  table->append({2, 1, 2});
  table->append({3, 2, 0});
  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});

  // table should be valid before adding new values
  EXPECT_TRUE(check_constraints(table));

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  // table should be still valid after adding new values
  EXPECT_TRUE(check_constraints(table));
}

TEST_F(ConstraintsTest, InvalidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  table->add_unique_constraint({ColumnID{0}});
  table->append({1, 1, 3});
  table->append({2, 1, 2});
  table->append({3, 2, 0});
  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // table should be valid before adding new values
  EXPECT_TRUE(check_constraints(table));

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->commit();

  // table should be invalid after adding new values
  EXPECT_FALSE(check_constraints(table));
}

// TODO question:
// should we also test inserting with the other variations of unique constraint?

/*
TEST_F(ConstraintsTest, InvalidConcatenatedInsert) {
  // Use private table to not interfere with other test cases
  TableColumnDefinitions table_column_definitions;
  table_column_definitions.emplace_back("column_1", DataType::Int, false);
  table_column_definitions.emplace_back("column_2", DataType::Int, false);

  auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  table->add_unique_constraint({ColumnID{0}, ColumnID{1}});

  table->append({1, 1});
  table->append({2, 1});
  table->append({1, 2});
  table->append({0, -1});

  // Test if first column is valid before the insert
  EXPECT_TRUE(check_constraints(table));

  table->append({1, 2});
  EXPECT_FALSE(check_constraints(table));

  // Test if enforcer recognizes duplicates
  EXPECT_FALSE(check_constraints(table));
}
*/

}  // namespace opossum
