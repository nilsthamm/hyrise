#include "operator_scan_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT
using namespace opossum::expression_factory;  // NOLINT

std::optional<AllParameterVariant> resolve_all_parameter_variant(const AbstractExpression& expression,
                                                                 const AbstractLQPNode& node) {
  auto value = AllParameterVariant{};
  auto value_column_id = std::optional<ColumnID>{};

  if (const auto* value_expression = dynamic_cast<const ValueExpression*>(&expression); value_expression) {
    value = value_expression->value;
  } else if (value_column_id = node.find_column_id(expression); value_column_id) {
    value = *value_column_id;
  } else if (const auto parameter_expression = dynamic_cast<const ParameterExpression*>(&expression); parameter_expression) {
    value = parameter_expression->parameter_id;
  } else {
    return std::nullopt;
  }

  return value;
}

}  // namespace

namespace opossum {

std::optional<std::vector<OperatorScanPredicate>> OperatorScanPredicate::from_expression(const AbstractExpression& expression,
                                                                    const AbstractLQPNode& node) {
  const auto* predicate = dynamic_cast<const AbstractPredicateExpression*>(&expression);
  if (!predicate) return std::nullopt;

  Assert(!predicate->arguments.empty(), "Expect PredicateExpression to have one or more arguments");

  auto predicate_condition = predicate->predicate_condition;

  // Split up the redundant abomination that is BETWEEN into two expressions
  if (predicate_condition == PredicateCondition::Between) {
    Assert(predicate->arguments.size() == 3, "Expect ternary PredicateExpression to have three arguments");

    auto lower_bound_predicates = from_expression(*greater_than_equals(predicate->arguments[0], predicate->arguments[1]), node);
    auto upper_bound_predicates = from_expression(*less_than_equals(predicate->arguments[0], predicate->arguments[2]), node);

    if (!lower_bound_predicates || !upper_bound_predicates) return std::nullopt;

    auto predicates = *lower_bound_predicates;
    predicates.insert(predicates.end(), upper_bound_predicates->begin(), upper_bound_predicates->end());

    return predicates;
  }

  auto argument_a = resolve_all_parameter_variant(*predicate->arguments[0], node);
  if (!argument_a) return std::nullopt;


  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    if (is_column_id(*argument_a)) {
      return std::vector<OperatorScanPredicate>{OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition}};
    } else {
      return std::nullopt;
    }
  }

  Assert(predicate->arguments.size() > 1, "Expect non-unary PredicateExpression to have two or more arguments");

  auto argument_b = resolve_all_parameter_variant(*expression.arguments[1], node);
  if (!argument_b) return std::nullopt;

  if (!is_column_id(*argument_a) && is_column_id(*argument_b)) {
    std::swap(argument_a, argument_b);
    predicate_condition = flip_predicate_condition(predicate_condition);
  }

  return std::vector<OperatorScanPredicate>{OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition, *argument_b}};
}

OperatorScanPredicate::OperatorScanPredicate(const ColumnID column_id, const PredicateCondition predicate_condition,
                                     const AllParameterVariant& value)
    : column_id(column_id), predicate_condition(predicate_condition), value(value) {}

}  // namespace opossum