#include "micro_benchmark_constraint_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace {
// Generating a table with 1,000,000 rows, a chunk size of 100,000 results in 10 chunks per table
constexpr auto CHUNK_SIZE = opossum::ChunkID{100'000};
constexpr int NUM_ROWS = 1'000'000;
}  // namespace

namespace opossum {

void MicroBenchmarkConstraintFixture::SetUp(::benchmark::State& state) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column0", DataType::Int, true);
  column_definitions.emplace_back("column1", DataType::Int, true);

  auto table_temp = std::make_shared<Table>(column_definitions, TableType::Data, CHUNK_SIZE, UseMvcc::Yes);
  auto& manager = StorageManager::get();
  manager.add_table("table_temp", table_temp);

  for (int row_id = 0; row_id < NUM_ROWS; row_id++) {
    table_temp->append({row_id, row_id});
  }

  auto gt = std::make_shared<GetTable>("table_temp");
  gt->execute();

  auto table_with_constraint = std::make_shared<Table>(column_definitions, TableType::Data, CHUNK_SIZE, UseMvcc::Yes);
  auto table_without_constraint = std::make_shared<Table>(
    column_definitions, TableType::Data, CHUNK_SIZE, UseMvcc::Yes);
  table_with_constraint->add_unique_constraint({ColumnID{0}});
  manager.add_table("table_with_constraint", table_with_constraint);
  manager.add_table("table_without_constraint", table_without_constraint);
  auto table_insert_w_c = std::make_shared<Insert>("table_with_constraint", gt);
  auto table_insert_wo_c = std::make_shared<Insert>("table_without_constraint", gt);
  auto table_context = TransactionManager::get().new_transaction_context();
  table_insert_w_c->set_transaction_context(table_context);
  table_insert_w_c->execute();
  table_insert_wo_c->set_transaction_context(table_context);
  table_insert_wo_c->execute();
  table_context->commit();

  _table_wrapper_w_c = std::make_shared<TableWrapper>(table_with_constraint);
  _table_wrapper_wo_c = std::make_shared<TableWrapper>(table_without_constraint);

  _table_wrapper_w_c->execute();
  _table_wrapper_wo_c->execute();
}

void MicroBenchmarkConstraintFixture::TearDown(::benchmark::State&) { opossum::StorageManager::get().reset(); }

void MicroBenchmarkConstraintFixture::_clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}  // namespace opossum
