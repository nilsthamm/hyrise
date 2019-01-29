#include <memory>

#include "benchmark/benchmark.h"
#include "types.hpp"

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class MicroBenchmarkConstraintFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override;
  void TearDown(::benchmark::State&) override;

 protected:
  void _clear_cache();

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_w_c;
  std::shared_ptr<TableWrapper> _table_wrapper_wo_c;
};

}  // namespace opossum
