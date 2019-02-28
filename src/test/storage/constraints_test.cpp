#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

std::tuple<std::shared_ptr<Insert>, std::shared_ptr<TransactionContext>> _insert_values(
    std::string table_name, std::shared_ptr<Table> new_values) {
  auto& manager = StorageManager::get();
  manager.add_table("new_values", new_values);

  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>(table_name, gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();

  manager.drop_table("new_values");

  return std::make_tuple(ins, context);
}

class ConstraintsTest : public BaseTest {
 protected:
  void SetUp() override {
    // First a test table with nonnullable columns is created. This table can be reused in all test as a base table.
    column_definitions.emplace_back("column0", DataType::Int);
    column_definitions.emplace_back("column1", DataType::Int);
    column_definitions.emplace_back("column2", DataType::Int);
    column_definitions.emplace_back("column3", DataType::Int);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& manager = StorageManager::get();
    manager.add_table("table", table);

    // The values are added with an insert operator to generate MVCC data.
    auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    new_values->append({1, 1, 3, 1});
    new_values->append({2, 1, 2, 1});
    new_values->append({3, 2, 0, 2});

    auto[_1, context1] = _insert_values("table", new_values);
    context1->commit();

    // Initially a unique constraint is defined on a single column since this can be used in all tests
    table->add_unique_constraint({ColumnID{0}});

    // Next, a test table with nullable columns is created. This table can be reused in all test as a base table
    nullable_column_definitions.emplace_back("column0", DataType::Int, true);
    nullable_column_definitions.emplace_back("column1", DataType::Int, true);
    nullable_column_definitions.emplace_back("column2", DataType::Int, true);
    nullable_column_definitions.emplace_back("column3", DataType::Int, true);

    auto table_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
    manager.add_table("table_nullable", table_nullable);

    auto[_2, context2] = _insert_values("table_nullable", new_values);
    context2->commit();

    // Initially one for one column a unique constraint is defined since this can be used in all tests
    table_nullable->add_unique_constraint({ColumnID{0}});
  }

  TableColumnDefinitions column_definitions;
  TableColumnDefinitions nullable_column_definitions;
};

void _add_concatenated_constraint(std::string table_name) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table(table_name);
  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
}

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto table_nullable = manager.get_table("table_nullable");

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_unique_constraint({ColumnID{5}}), std::exception);

  // Invalid because the constraint contains duplicated columns.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}, ColumnID{1}}), std::exception);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{1}}, true), std::exception);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{1}}, true), std::exception);

  // Invalid because the column contains duplicated values.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{1}}), std::exception);

  table->add_unique_constraint({ColumnID{2}}, true);

  // Invalid because another primary key already exists.
  EXPECT_THROW(
      {
        try {
          table->add_unique_constraint({ColumnID{2}}, true);
        } catch (const std::exception& e) {
          // and this tests that it has the correct message
          EXPECT_TRUE(std::string(e.what()).find("Another primary key already exists for this table.") !=
                      std::string::npos);
          throw;
        }
      },
      std::exception);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}}), std::exception);

  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(table->add_unique_constraint({ColumnID{0}, ColumnID{2}}), std::exception);
}

TEST_F(ConstraintsTest, ValidInsert) {
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

  // Only values that are not yet in column 0 (has a unique constraint) are added to the column
  new_values->append({6, 42, 42, 42});
  new_values->append({4, 42, 42, 42});

  auto[ins, context] = _insert_values("table", new_values);

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsert) {
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

  // A new value and an already existing value are added to column 0, which has a unique constraint
  new_values->append({6, 42, 42, 42});
  new_values->append({3, 42, 42, 42});

  auto[ins, context] = _insert_values("table", new_values);

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, InvalidInsertOnDict) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");

  // On dictionary segments, an optimization is used to skip them if the value is not in the dictionary.
  // Therefore it is necessary to test if this does not skip values unintendedly.
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});

  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

  // The value 1 is already in a compressed segment of column 1
  new_values->append({6, 42, 42, 42});
  new_values->append({1, 42, 42, 42});

  auto[ins, context] = _insert_values("table", new_values);

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertNullable) {
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);

  // Two new values and two null values are added to column 0, which has a unique constraint
  // This is valid since a unique constraint defines that only non null values must be unique
  new_values->append({6, 42, 42, 42});
  new_values->append({4, 42, 42, 42});
  new_values->append({NullValue{}, 42, 42, 42});
  new_values->append({NullValue{}, 42, 42, 42});

  auto[ins, context] = _insert_values("table_nullable", new_values);

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertNullable) {
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);

  // The same as the test before with the difference that now one of the added values already is in the column
  new_values->append({6, 42, 42, 42});
  new_values->append({2, 42, 42, 42});
  new_values->append({NullValue{}, 42, 42, 42});

  auto[ins, context] = _insert_values("table_nullable", new_values);

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertConcatenated) {
  _add_concatenated_constraint("table");

  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  // Although the value 0 already exists in column 2, we can still add it, as (6,0) does not exist in columns 0 and 2.
  new_values->append({6, 42, 0, 42});
  new_values->append({4, 42, 4, 42});

  auto[ins, context] = _insert_values("table", new_values);

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertConcatenated) {
  _add_concatenated_constraint("table");

  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  // The tuple (3,0) already exists in the columns 0 and 2
  new_values->append({3, 42, 0, 42});
  new_values->append({4, 42, 3, 42});

  auto[ins, context] = _insert_values("table", new_values);

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, ValidInsertNullableConcatenated) {
  _add_concatenated_constraint("table_nullable");

  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  // It is valid to add the null value tuple twice since unique constraints only enforce non null values
  new_values->append({6, 42, 1, 42});
  new_values->append({4, 42, 4, 42});
  new_values->append({NullValue{}, 1, NullValue{}, 42});
  new_values->append({NullValue{}, 1, NullValue{}, 42});

  auto[ins, context] = _insert_values("table_nullable", new_values);

  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsTest, InvalidInsertNullableConcatenated) {
  _add_concatenated_constraint("table_nullable");

  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  // The tuple (3,0) already exists in the columns 0 and 2
  new_values->append({3, 42, 0, 42});
  new_values->append({4, 42, 3, 42});

  auto[ins, context] = _insert_values("table_nullable", new_values);

  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsTest, InvalidInsertDeleteRace) {
  // This test simulates the situation where one transaction wants to add an already existing
  // value that is deleted by another transaction at the same time. Both operators only commit
  // successfully if the delete operator COMMITS before the insert operator is EXECUTED

  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

  new_values->append({3, 42, 1, 42});
  new_values->append({4, 42, 3, 42});

  // Add new values but do NOT commit
  auto[insert, insert_context] = _insert_values("table", new_values);

  EXPECT_TRUE(insert->execute_failed());
  EXPECT_TRUE(insert_context->rollback());

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(del_transaction_context);
  validate->execute();
  auto table_scan = create_table_scan(validate, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  EXPECT_TRUE(del_transaction_context->commit());
}

TEST_F(ConstraintsTest, ValidInsertDeleteRace) {
  // This test simulates the situation where one transaction wants to add an already existing
  // value that is deleted by another transaction at the same time. Both operators only commit
  // successfully if the delete operator COMMITS before the insert operator is EXECUTED
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  new_values->append({3, 42, 1, 42});
  new_values->append({4, 42, 3, 42});

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value and commit directly
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(del_transaction_context);
  validate->execute();
  auto table_scan = create_table_scan(validate, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();

  EXPECT_FALSE(delete_op->execute_failed());
  EXPECT_TRUE(del_transaction_context->commit());

  auto[insert, insert_context] = _insert_values("table", new_values);

  EXPECT_FALSE(insert->execute_failed());
  EXPECT_TRUE(insert_context->commit());
}

TEST_F(ConstraintsTest, InsertInsertRace) {
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  new_values->append({5, 42, 1, 42});

  // They both execute successfully since the value was not commited by either of them at the point of execution
  auto[insert_1, insert_1_context] = _insert_values("table", new_values);
  EXPECT_FALSE(insert_1->execute_failed());
  auto[insert_2, insert_2_context] = _insert_values("table", new_values);
  EXPECT_FALSE(insert_2->execute_failed());

  // Only the first commit is successfully, the other transaction sees the inserted value at the point of commiting
  EXPECT_TRUE(insert_1_context->commit());
  EXPECT_FALSE(insert_2_context->commit());
  EXPECT_TRUE(insert_2_context->rollback());
}

}  // namespace opossum
