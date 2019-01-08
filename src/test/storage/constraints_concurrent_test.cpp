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
  }

  auto t1_operator() {
    auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    StorageManager::get().add_table("new_values", new_values);
    new_values->append({3, 42, 42});
    auto gt = std::make_shared<GetTable>("new_values");
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
};

TEST_F(ConstraintsConcurrentTest, ValidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({6, 0, 1});
  new_values->append({4, 1, 3});

  // table should be valid before adding new values
  // EXPECT_TRUE(check_constraints(table));

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(context->commit());

  // table should be still valid after adding new values
  // EXPECT_TRUE(check_constraints(table));
}

TEST_F(ConstraintsConcurrentTest, Valid2Insert2) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // table should be valid before adding new values
  // EXPECT_TRUE(check_constraints(table));

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());

  // table should be invalid after adding new values
  // EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsConcurrentTest, InvalidInsert) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // table should be valid before adding new values
  // EXPECT_TRUE(check_constraints(table));

  // add new values
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  EXPECT_TRUE(ins->execute_failed());
  context->rollback();

  // table should be invalid after adding new values
  // EXPECT_FALSE(check_constraints(table));
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
  auto ins = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(insert_context);
  ins->execute();

    auto get_table = std::make_shared<GetTable>("table");
    get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);

  EXPECT_FALSE(insert_context->commit());
  del_transaction_context->commit();

  // table should be invalid after adding new values
  // EXPECT_FALSE(check_constraints(table));
}

TEST_F(ConstraintsConcurrentTest, ValidInsertDeleteRace) {
  auto& manager = StorageManager::get();
  auto table = manager.get_table("table");
  auto new_values = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
  manager.add_table("new_values", new_values);

  new_values->append({3, 0, 1});
  new_values->append({4, 1, 3});

  // add new values but do NOT commit
  auto gt = std::make_shared<GetTable>("new_values");
  gt->execute();
  auto ins = std::make_shared<Insert>("table", gt);
  auto insert_context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(insert_context);
  ins->execute();
  
    auto get_table = std::make_shared<GetTable>("table");
    get_table->execute();

  // create delete op for later added already existing value but do NOT commit
  auto del_transaction_context = TransactionManager::get().new_transaction_context();
  auto table_scan = create_table_scan(get_table, ColumnID{0}, PredicateCondition::Equals, "3");
  table_scan->execute();
  auto delete_op = std::make_shared<Delete>("table", table_scan);
  delete_op->set_transaction_context(del_transaction_context);

  del_transaction_context->commit();

  insert_context->commit();

  // table should be valid after adding new values
  // EXPECT_TRUE(check_constraints(table));
}

TEST_F(ConstraintsConcurrentTest, InsertInsertRace) {
  auto insert_1 = t1_operator();
  auto insert_2 = t1_operator();
  auto insert_1_context = TransactionManager::get().new_transaction_context();
  auto insert_2_context = TransactionManager::get().new_transaction_context();
  insert_1->set_transaction_context(insert_1_context);
  insert_2->set_transaction_context(insert_2_context);
  insert_1->execute();
  insert_2->execute();

  EXPECT_TRUE(insert_2_context->commit());
  EXPECT_FALSE(insert_1_context->commit());
}

/*
 * In the following we always look at these three transactions:
 * t1: inserts values (3,42,42)
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
  auto t1 = t1_operator();
  auto t1_context = TransactionManager::get().new_transaction_context();
  t1->set_transaction_context(t1_context);
  t1->execute();

  auto t2 = t2_operator();
  auto t2_context = TransactionManager::get().new_transaction_context();
  t2->set_transaction_context(t2_context);
  t2->execute();

  auto t4 = t4_operator();
  auto t4_context = TransactionManager::get().new_transaction_context();
  t4->set_transaction_context(t4_context);
  t4->execute();

  EXPECT_FALSE(t2_context->commit());
  t2_context->rollback();
  EXPECT_TRUE(t4_context->commit());
  EXPECT_TRUE(t1_context->commit());
}

// execute: [t1,t2,t3,t4] - commit order: T3 success, T2 -> success, T4 fail, T1 fail
TEST_F(ConstraintsConcurrentTest, TripleConcurrentRaceCaseB) {
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
}

// execute: [t1,t2,t3,t4] - commit order: T3 success, T2 -> success, T4 fail, T1 fail
TEST_F(ConstraintsConcurrentTest, TripleConcurrentRaceCaseC) {
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
}

}  // namespace opossum
