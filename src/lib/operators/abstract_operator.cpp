#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/print_directed_acyclic_graph.hpp"
#include "utils/timer.hpp"
#include "jit_evaluation_helper.hpp"
// #include <papi.h>

namespace opossum {

AbstractOperator::AbstractOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right)
    : _type(type), _input_left(left), _input_right(right) {}

OperatorType AbstractOperator::type() const { return _type; }

void AbstractOperator::execute() {
  DebugAssert(!_input_left || _input_left->get_output(), "Left input has not yet been executed");
  DebugAssert(!_input_right || _input_right->get_output(), "Right input has not yet been executed");
  DebugAssert(!_output, "Operator has already been executed");

  auto& result = JitEvaluationHelper::get().result();

  auto papi_events = JitEvaluationHelper::get().globals()["papi_events"];
  auto num_counters = papi_events.size();
  // int32_t papi_event_ids[10];
  long long papi_values[10];

  for (uint32_t i = 0; i < num_counters; ++i) {
    // if (PAPI_event_name_to_code(papi_events[i].get<std::string>().c_str(), &papi_event_ids[i]) < 0) throw std::logic_error("PAPI error");
  }

  Timer performance_timer;
  if (num_counters) {
    // //  if (PAPI_assign_eventset_component(papi_event_ids, 0) < 0) throw std::logic_error("PAPI error");
    // if (PAPI_start_counters(papi_event_ids, num_counters) < 0) throw std::logic_error("PAPI error " + std::to_string(PAPI_start_counters(papi_event_ids, num_counters)));
  }
  _prepare();
  if (num_counters) {
    // if (PAPI_stop_counters(papi_values, num_counters) < 0) throw std::logic_error("PAPI error");
  }

  auto walltime_ns = performance_timer.lap().count();
  nlohmann::json op = {{"name", name()}, {"prepare", true}, {"walltime", walltime_ns / 1000.0}};
  for (uint32_t i = 0; i < num_counters; ++i) {
    op[papi_events[i].get<std::string>()] = papi_values[i];
    papi_values[i] = 0;
  }
  result["operators"].push_back(op);

  performance_timer.lap();

  if (num_counters) {
    // if (PAPI_start_counters(papi_event_ids, num_counters) < 0) throw std::logic_error("PAPI error");
  }

  auto transaction_context = this->transaction_context();

  if (transaction_context) {
    /**
     * Do not execute Operators if transaction has been aborted.
     * Not doing so is crucial in order to make sure no other
     * tasks of the Transaction run while the Rollback happens.
     */
    if (transaction_context->aborted()) {
      return;
    }
    transaction_context->on_operator_started();
    _output = _on_execute(transaction_context);
    transaction_context->on_operator_finished();
  } else {
    _output = _on_execute(nullptr);
  }

  // release any temporary data if possible
  _on_cleanup();

  if (num_counters) {
    // if (PAPI_stop_counters(papi_values, num_counters) < 0) throw std::logic_error("PAPI error");
  }

  _base_performance_data.walltime = performance_timer.lap();

  nlohmann::json op2 = {{"name", name()}, {"prepare", false}, {"walltime", _base_performance_data.walltime.count() / 1000.0}};
  for (uint32_t i = 0; i < num_counters; ++i) {
    op2[papi_events[i].get<std::string>()] = papi_values[i];
  }
  result["operators"].push_back(op2);
}

// returns the result of the operator
std::shared_ptr<const Table> AbstractOperator::get_output() const {
  DebugAssert(
      [&]() {
        if (_output == nullptr) return true;
        if (_output->chunk_count() <= ChunkID{1}) return true;
        for (auto chunk_id = ChunkID{0}; chunk_id < _output->chunk_count(); ++chunk_id) {
          if (_output->get_chunk(chunk_id)->size() < 1) return true;
        }
        return true;
      }(),
      "Empty chunk returned from operator " + description());

  DebugAssert(!_output || _output->column_count() > 0, "Operator " + description() + " did not output any columns");

  return _output;
}

void AbstractOperator::clear_output() { _output = nullptr; }

const std::string AbstractOperator::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> AbstractOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>> recreated_ops;
  return _recreate_impl(recreated_ops, args);
}

std::shared_ptr<const Table> AbstractOperator::input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::input_table_right() const { return _input_right->get_output(); }

bool AbstractOperator::transaction_context_is_set() const { return _transaction_context.has_value(); }

std::shared_ptr<TransactionContext> AbstractOperator::transaction_context() const {
  DebugAssert(!transaction_context_is_set() || !_transaction_context->expired(),
              "TransactionContext is expired, but SQL Query Executor should still own it (Operator: " + name() + ")");
  return transaction_context_is_set() ? _transaction_context->lock() : nullptr;
}

void AbstractOperator::set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
}

void AbstractOperator::set_transaction_context_recursively(
    const std::weak_ptr<TransactionContext>& transaction_context) {
  set_transaction_context(transaction_context);

  if (_input_left != nullptr) mutable_input_left()->set_transaction_context_recursively(transaction_context);
  if (_input_right != nullptr) mutable_input_right()->set_transaction_context_recursively(transaction_context);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_left() const {
  return std::const_pointer_cast<AbstractOperator>(_input_left);
}

std::shared_ptr<AbstractOperator> AbstractOperator::mutable_input_right() const {
  return std::const_pointer_cast<AbstractOperator>(_input_right);
}

const BaseOperatorPerformanceData& AbstractOperator::base_performance_data() const { return _base_performance_data; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_left() const { return _input_left; }

std::shared_ptr<const AbstractOperator> AbstractOperator::input_right() const { return _input_right; }

void AbstractOperator::print(std::ostream& stream) const {
  const auto get_children_fn = [](const auto& op) {
    std::vector<std::shared_ptr<const AbstractOperator>> children;
    if (op->input_left()) children.emplace_back(op->input_left());
    if (op->input_right()) children.emplace_back(op->input_right());
    return children;
  };
  const auto node_print_fn = [](const auto& op, auto& stream) {
    stream << op->description();

    // If the operator was already executed, print some info about data and performance
    const auto output = op->get_output();
    if (output) {
      stream << " (" << output->row_count() << " row(s)/" << output->chunk_count() << " chunk(s)/"
             << output->column_count() << " column(s)/";

      stream << format_bytes(output->estimate_memory_usage());
      stream << "/";
      stream << format_duration(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(op->base_performance_data().walltime))
             << ")";
    }
  };

  print_directed_acyclic_graph<const AbstractOperator>(shared_from_this(), get_children_fn, node_print_fn, stream);
}

void AbstractOperator::_on_cleanup() {}

std::shared_ptr<AbstractOperator> AbstractOperator::_recreate_impl(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& recreated_ops,
    const std::vector<AllParameterVariant>& args) const {
  const auto recreated_ops_iter = recreated_ops.find(this);
  if (recreated_ops_iter != recreated_ops.end()) return recreated_ops_iter->second;

  const auto recreated_input_left =
      input_left() ? input_left()->_recreate_impl(recreated_ops, args) : std::shared_ptr<AbstractOperator>{};
  const auto recreated_input_right =
      input_right() ? input_right()->_recreate_impl(recreated_ops, args) : std::shared_ptr<AbstractOperator>{};

  const auto recreated_op = _on_recreate(args, recreated_input_left, recreated_input_right);
  recreated_ops.emplace(this, recreated_op);

  return recreated_op;
}

}  // namespace opossum
