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

// TODO tests of transactions conflicting each other
// possibly in another file

}  // namespace opossum
