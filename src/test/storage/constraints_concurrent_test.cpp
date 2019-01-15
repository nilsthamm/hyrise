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
#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
class ConstraintsConcurrentTest: public BaseTest {
 protected:
  void SetUp() override {
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    column_definitions.emplace_back("column2", DataType::Int, false);

    auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    auto& manager = StorageManager::get();
    manager.add_table("table_temp", table_temp);

    table_temp->append({1, 1, 3});
    table_temp->append({2, 1, 2});
    table_temp->append({3, 2, 0});

    auto gt = std::make_shared<GetTable>("table_temp");
    gt->execute();

    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
    table->add_unique_constraint({ColumnID{0}});
    manager.add_table("table", table);
    auto table_insert = std::make_shared<Insert>("table", gt);
    auto table_context = TransactionManager::get().new_transaction_context();
    table_insert->set_transaction_context(table_context);
    table_insert->execute();
    table_context->commit();

    nullable_column_definitions.emplace_back("column0", DataType::Int, true);
    nullable_column_definitions.emplace_back("column1", DataType::Int, false);
    nullable_column_definitions.emplace_back("column2", DataType::Int, false);

    auto table_temp_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 3, UseMvcc::Yes);
    manager.add_table("table_temp_nullable", table_temp_nullable);

    table_temp_nullable->append({1, 1, 3});
    table_temp_nullable->append({2, 1, 2});
    table_temp_nullable->append({3, 2, 0});

    auto gt_nullable = std::make_shared<GetTable>("table_temp_nullable");
    gt_nullable->execute();

    auto table_nullable = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 3, UseMvcc::Yes);
    table_nullable->add_unique_constraint({ColumnID{0}});
    manager.add_table("table_nullable", table_nullable);
    auto table_nullable_insert = std::make_shared<Insert>("table_nullable", gt_nullable);
    auto table_nullable_context = TransactionManager::get().new_transaction_context();
    table_nullable_insert->set_transaction_context(table_nullable_context);
    table_nullable_insert->execute();
    table_nullable_context->commit();
  }

  auto t1_operator() {
    auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    StorageManager::get().add_table("t1", new_values);
    new_values->append({9, 42, 42});
    auto gt = std::make_shared<GetTable>("t1");
    gt->execute();
    return std::make_shared<Insert>("table", gt);
  }

  auto t2_operator() {
    auto get_table = std::make_shared<GetTable>("table");
    get_table->execute();
    auto where_one_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "1");
    where_one_scan->execute();
    auto column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "column0");
    auto updated_values_projection = std::make_shared<Projection>(where_one_scan, expression_vector(column_a, 3));
    updated_values_projection->execute();
    return std::make_shared<Update>("table", where_one_scan, updated_values_projection);
  }

  auto t3_operator() {
    auto get_table = std::make_shared<GetTable>("table");
    get_table->execute();
    auto where_one_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
    where_one_scan->execute();
    auto column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "column0");
    auto updated_values_projection = std::make_shared<Projection>(where_one_scan, expression_vector(column_a, 42));
    updated_values_projection->execute();
    return std::make_shared<Update>("table", where_one_scan, updated_values_projection);
  }

  auto t4_operator() {
    auto get_table = std::make_shared<GetTable>("table");
    get_table->execute();
    auto where_three_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
    where_three_scan->execute();
    return std::make_shared<Delete>("table", where_three_scan);
  }


  TableColumnDefinitions column_definitions;
  TableColumnDefinitions nullable_column_definitions;
};

TEST_F(ConstraintsConcurrentTest, ValidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsConcurrentTest, InvalidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsConcurrentTest, ValidInsertNullable) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});
  new_values->append({NullValue{}, 1, 3});
  new_values->append({NullValue{}, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsConcurrentTest, InvalidInsertNullable) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({NullValue{}, 1, 3});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsConcurrentTest, ValidInsertConcatenated) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 4});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsConcurrentTest, InvalidInsertConcatenated) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  table->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsConcurrentTest, ValidInsertNullableConcatenated) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 4});
  new_values->append({NullValue{}, 1, 5});
  new_values->append({NullValue{}, 1, 6});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_FALSE(ins->execute_failed());
  EXPECT_TRUE(context->commit());
}

TEST_F(ConstraintsConcurrentTest, InvalidInsertNullableConcatenated) {
  auto& manager = StorageManager::get();
  auto table_nullable = manager.get_table("table_nullable");
  table_nullable->add_unique_constraint({ColumnID{0}, ColumnID{2}});
  auto new_values = std::make_shared<Table>(nullable_column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 5});
  new_values->append({1, 1, 3});

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table_nullable", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());
  EXPECT_TRUE(context->rollback());
}

TEST_F(ConstraintsConcurrentTest, InvalidInsertDeleteRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values but do NOT commit
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  insert->set_transaction_context(insert_context);
  insert->execute();
  EXPECT_TRUE(insert->execute_failed());
  EXPECT_TRUE(insert_context->rollback());

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  EXPECT_TRUE(del_transaction_context->commit());
}

TEST_F(ConstraintsConcurrentTest, ValidInsertDeleteRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  auto get_table = std::make_shared<GetTable>("table");
  get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);
  delete_op->execute();
  EXPECT_FALSE(delete_op->execute_failed());

  EXPECT_TRUE(del_transaction_context->commit());

  // add new values but do NOT commit
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  insert->set_transaction_context(insert_context);
  insert->execute();
  EXPECT_FALSE(insert->execute_failed());
  EXPECT_TRUE(insert_context->commit());
}

TEST_F(ConstraintsConcurrentTest, InsertInsertRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);
  new_values->append({5, 0, 1});

  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto insert_1 = std::make_shared<Insert>("table", gt);
  auto insert_2 = std::make_shared<Insert>("table", gt);

  auto insert_1_context = TransactionManager::get().new_transaction_context();
  auto insert_2_context = TransactionManager::get().new_transaction_context();
  insert_1->set_transaction_context(insert_1_context);
  insert_2->set_transaction_context(insert_2_context);
  insert_1->execute();
  EXPECT_FALSE(insert_1->execute_failed());
  insert_2->execute();
  EXPECT_FALSE(insert_2->execute_failed());

  EXPECT_TRUE(insert_1_context->commit());
  EXPECT_FALSE(insert_2_context->commit());
  EXPECT_TRUE(insert_2_context->rollback());
}

/*
 * In the following we always look at these three transactions:
 * t1: inserts values (9,42,42)
 * t2: updates all rows where column0 = 1 to 3
 * t3: updates all rows where column0 = 3 to 42
 * t4: deletes all rows where column0 = 3
 *
 * Depending on the test case all these operators are executed and commited in different order
 * 
 * TestCases A to D work on a table where the value 3 is already contained in column0
 * TestCases E to G work on a table where the value 3 is not contained in column0
 */


// execute: [t1,t2,t4] - commit order t2 -> fail; t4 -> success; t1 -> success 
TEST_F(ConstraintsConcurrentTest, TripleConcurrentRaceCaseA) {
  /*
  auto t1 = t1_operator("t1");
  auto t1_context = TransactionManager::get().new_transaction_context();
  t1->set_transaction_context(t1_context);
  t1->execute();
  EXPECT_FALSE(t1->execute_failed());

  auto t2 = t2_operator();
  auto t2_context = TransactionManager::get().new_transaction_context();
  t2->set_transaction_context(t2_context);
  t2->execute();
  EXPECT_FALSE(t2->execute_failed());

  auto t4 = t4_operator();
  auto t4_context = TransactionManager::get().new_transaction_context();
  t4->set_transaction_context(t4_context);
  t4->execute();
  EXPECT_FALSE(t4->execute_failed());

  EXPECT_FALSE(t2_context->commit());
  EXPECT_TRUE(t2_context->rollback());
  EXPECT_TRUE(t4_context->commit());
  EXPECT_TRUE(t1_context->commit());
  */
}

// execute: [t1,t2,t3,t4] - commit order: T3 success, T2 -> success, T4 fail, T1 fail
TEST_F(ConstraintsConcurrentTest, TripleConcurrentRaceCaseB) {
  /*
  auto t1 = t1_operator();
  auto t1_context = TransactionManager::get().new_transaction_context();
  t1->set_transaction_context(t1_context);
  t1->execute();
  EXPECT_FALSE(t1->execute_failed());

  auto t2 = t2_operator();
  auto t2_context = TransactionManager::get().new_transaction_context();
  t2->set_transaction_context(t2_context);
  t2->execute();
  EXPECT_FALSE(t2->execute_failed());

  auto t3 = t3_operator();
  auto t3_context = TransactionManager::get().new_transaction_context();
  t3->set_transaction_context(t3_context);
  t3->execute();
  EXPECT_FALSE(t3->execute_failed());

  auto t4 = t4_operator();
  auto t4_context = TransactionManager::get().new_transaction_context();
  t4->set_transaction_context(t4_context);
  t4->execute();
  EXPECT_FALSE(t4->execute_failed());

  EXPECT_TRUE(t3_context->commit());
  EXPECT_TRUE(t2_context->commit());
  EXPECT_FALSE(t4_context->commit());
  EXPECT_TRUE(t4_context->rollback());
  EXPECT_FALSE(t1_context->commit());
  EXPECT_TRUE(t1_context->rollback());
  */
}

// execute: [t1,t2,t3,t4] - commit order: T3 success, T2 -> success, T4 fail, T1 fail
TEST_F(ConstraintsConcurrentTest, TripleConcurrentRaceCaseC) {
  /*
  auto t1 = t1_operator();
  auto t1_context = TransactionManager::get().new_transaction_context();
  t1->set_transaction_context(t1_context);
  t1->execute();

  auto t2 = t2_operator();
  auto t2_context = TransactionManager::get().new_transaction_context();
  t2->set_transaction_context(t2_context);
  t2->execute();

  auto t3 = t3_operator();
  auto t3_context = TransactionManager::get().new_transaction_context();
  t3->set_transaction_context(t3_context);
  t3->execute();

  auto t4 = t4_operator();
  auto t4_context = TransactionManager::get().new_transaction_context();
  t4->set_transaction_context(t4_context);
  t4->execute();

  EXPECT_TRUE(t3_context->commit());
  EXPECT_TRUE(t2_context->commit());
  EXPECT_FALSE(t4_context->commit());
  t4_context->rollback();
  EXPECT_FALSE(t1_context->commit());
  t1_context->rollback();
  */
}

}  // namespace opossum
