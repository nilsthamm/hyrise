#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "operators/delete.hpp"
#include "operators/insert.hpp"
#include "operators/validate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/jit_operator_wrapper.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "types.hpp"


int main(int argc, char* argv[]) {
  try {
    uint16_t port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    auto table_a = opossum::load_table("src/test/tables/int_float.tbl", 2);
    opossum::StorageManager::get().add_table("table_a", table_a);

    auto table_b = opossum::load_table("src/test/tables/int.tbl", 1000);
    opossum::StorageManager::get().add_table("tmp", table_b);

    auto table_c = opossum::load_table("src/test/tables/int3.tbl", 1000);
    opossum::StorageManager::get().add_table("tmp2", table_c);


    auto context = opossum::TransactionManager::get().new_transaction_context();
    auto get_table = std::make_shared<opossum::GetTable>("tmp");
    // get_table->set_transaction_context(context);
    get_table->execute();

    auto table_scan = std::make_shared<opossum::TableScan>(get_table, opossum::ColumnID{0}, opossum::PredicateCondition::LessThan, 200);
    //table_scan->set_transaction_context(context);
    table_scan->execute();
    auto delete_op = std::make_shared<opossum::Delete>("tmp", table_scan);
    delete_op->set_transaction_context(context);
    delete_op->execute();
    context->commit();


    context = opossum::TransactionManager::get().new_transaction_context();
    table_scan = std::make_shared<opossum::TableScan>(get_table, opossum::ColumnID{0}, opossum::PredicateCondition::GreaterThan, 10000);
    //table_scan->set_transaction_context(context);
    table_scan->execute();
    delete_op = std::make_shared<opossum::Delete>("tmp", table_scan);
    delete_op->set_transaction_context(context);
    delete_op->execute();

    auto gt2 = std::make_shared<opossum::GetTable>("tmp2");
    gt2->execute();
    auto ins = std::make_shared<opossum::Insert>("tmp", gt2);
    ins->set_transaction_context(context);
    ins->execute();



    /* get_table = std::make_shared<opossum::GetTable>("tmp");
    get_table->set_transaction_context(context);
    get_table->execute(); */


    auto validate = std::make_shared<opossum::Validate>(get_table);
    validate->set_transaction_context(context);
    auto jit_operator = std::make_shared<opossum::JitOperatorWrapper>(get_table, opossum::JitExecutionMode::Compile); // Interpret validate
    auto read_tuple = std::make_shared<opossum::JitReadTuples>(false);
    opossum::JitTupleValue tuple_val = read_tuple->add_input_column(opossum::DataType::Int, false, opossum::ColumnID(0));
    jit_operator->add_jit_operator(read_tuple);
    jit_operator->add_jit_operator(std::make_shared<opossum::JitValidate>(context, false));

    auto id = 0; // read_tuple->add_temporary_value();

    auto expression = std::make_shared<opossum::JitExpression>(std::make_shared<opossum::JitExpression>(tuple_val),
                                                               opossum::ExpressionType::Addition, std::make_shared<opossum::JitExpression>(tuple_val), id);

    auto compute = std::make_shared<opossum::JitCompute>(expression);

    // jit_operator->add_jit_operator(compute);

    auto write_table = std::make_shared<opossum::JitWriteTuples>();
    // auto tuple_value = expression->result();
    write_table->add_output_column("a+a", tuple_val); // tuple_value
    jit_operator->add_jit_operator(write_table);

    jit_operator->set_transaction_context(context);
    auto print = std::make_shared<opossum::Print>(jit_operator);
    print->set_transaction_context(context);
    validate->execute();
    jit_operator->execute();
    print->execute();


    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));

    boost::asio::io_service io_service;

    // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
    // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
    // constructor and then runs forever.
    opossum::Server server{io_service, port};

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
