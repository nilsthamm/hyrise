#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

template <typename T>
class BasicHistogramTest : public BaseTest {
  void SetUp() override { _int_float4 = load_table("src/test/tables/int_float4.tbl"); }

 protected:
  std::shared_ptr<Table> _int_float4;
};

// TODO(tim): basic tests for float/double/int64
using HistogramTypes =
    ::testing::Types<EqualNumElementsHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(BasicHistogramTest, HistogramTypes);

TYPED_TEST(BasicHistogramTest, CanPruneLowerBound) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CanPruneUpperBound) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CannotPruneExistingValue) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
}

class HistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
    _string2 = load_table("src/test/tables/string2.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _expected_join_result_1;
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
};

TEST_F(HistogramTest, EqualNumElementsBasic) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 2u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 2.5f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsUnevenBuckets) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsFloat) {
  auto hist = EqualNumElementsHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.5f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsString) {
  auto hist = EqualNumElementsHistogram<std::string>(_string2);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("a", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aa", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ab", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("b", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("birne", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("biscuit", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("bla", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("blubb", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("bums", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ttt", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("turkey", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("uuu", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("vvv", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("www", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("xxx", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yyy", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzz", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzzzzz", PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsLessThan) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (123 - 12 + 1) * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan), 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 7.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 7.f);
}

TEST_F(HistogramTest, EqualNumElementsFloatLessThan) {
  auto hist = EqualNumElementsHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(2.2f, 2.2f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(2.2f, 2.2f + 1), PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(3.3f, 3.3f + 1), PredicateCondition::LessThan), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  4.f + 6.f + 4.f);
}

TEST_F(HistogramTest, EqualNumElementsStringLessThan) {
  auto hist = EqualNumElementsHistogram<std::string>(_string3, "abcdefghijklmnopqrstuvwxyz", 4u);
  hist.generate(ColumnID{0}, 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "efgh"
  constexpr auto bucket_1_upper = 4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7;
  // "ijkl"
  constexpr auto bucket_2_lower = 8 * 26 * 26 * 26 + 9 * 26 * 26 + 10 * 26 + 11;
  // "mnop"
  constexpr auto bucket_2_upper = 12 * 26 * 26 * 26 + 13 * 26 * 26 + 14 * 26 + 15;
  // "oopp"
  constexpr auto bucket_3_lower = 14 * 26 * 26 * 26 + 14 * 26 * 26 + 15 * 26 + 15;
  // "qrst"
  constexpr auto bucket_3_upper = 16 * 26 * 26 * 26 + 17 * 26 * 26 + 18 * 26 + 19;
  // "uvwx"
  constexpr auto bucket_4_lower = 20 * 26 * 26 * 26 + 21 * 26 * 26 + 22 * 26 + 23;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  constexpr auto bucket_1_count = 4.f;
  constexpr auto bucket_2_count = 6.f;
  constexpr auto bucket_3_count = 3.f;
  constexpr auto bucket_4_count = 3.f;
  constexpr auto total_count = bucket_1_count + bucket_2_count + bucket_3_count + bucket_4_count;

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aaaa", PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcd", PredicateCondition::LessThan), 0.f);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abce", PredicateCondition::LessThan), 1 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcf", PredicateCondition::LessThan), 2 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("cccc", PredicateCondition::LessThan),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("dddd", PredicateCondition::LessThan),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgg", PredicateCondition::LessThan),
                  (bucket_1_width - 2) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgh", PredicateCondition::LessThan),
                  (bucket_1_width - 1) / bucket_1_width * bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgi", PredicateCondition::LessThan), bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ijkl", PredicateCondition::LessThan), bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ijkm", PredicateCondition::LessThan),
                  1 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ijkn", PredicateCondition::LessThan),
                  2 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("jjjj", PredicateCondition::LessThan),
                  (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkk", PredicateCondition::LessThan),
                  (10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("lzzz", PredicateCondition::LessThan),
                  (11 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnoo", PredicateCondition::LessThan),
                  (bucket_2_width - 2) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnop", PredicateCondition::LessThan),
                  (bucket_2_width - 1) / bucket_2_width * bucket_2_count + bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnoq", PredicateCondition::LessThan), bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("oopp", PredicateCondition::LessThan), bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("oopq", PredicateCondition::LessThan),
                  1 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("oopr", PredicateCondition::LessThan),
                  2 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("pppp", PredicateCondition::LessThan),
                  (15 * 26 * 26 * 26 + 15 * 26 * 26 + 15 * 26 + 15 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qqqq", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qllo", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrss", PredicateCondition::LessThan),
                  (bucket_3_width - 2) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrst", PredicateCondition::LessThan),
                  (bucket_3_width - 1) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrsu", PredicateCondition::LessThan),
                  bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("uvwx", PredicateCondition::LessThan),
                  bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("uvwy", PredicateCondition::LessThan),
                  1 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("uvwz", PredicateCondition::LessThan),
                  2 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("vvvv", PredicateCondition::LessThan),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("xxxx", PredicateCondition::LessThan),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ycip", PredicateCondition::LessThan),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("yyzy", PredicateCondition::LessThan),
      (bucket_4_width - 2) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("yyzz", PredicateCondition::LessThan),
      (bucket_4_width - 1) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yzaa", PredicateCondition::LessThan), total_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzzz", PredicateCondition::LessThan), total_count);
}

TEST_F(HistogramTest, EqualWidthHistogramBasic) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4);
  hist.generate(ColumnID{1}, 6u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(11, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(14, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(15, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(17, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramUnevenBuckets) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4);
  hist.generate(ColumnID{1}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(11, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(14, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(15, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(17, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthFloat) {
  auto hist = EqualWidthHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.9f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.0f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.1f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.2f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.4f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.5f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthLessThan) {
  auto hist = EqualWidthHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  // The first bucket's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bucket_width = (123'456 - 12 + 1) / 3;

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan),
                  (1'234.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan),
                  (12'346.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(80'000, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan),
                  4.f + (123'456.f - (12 + 2 * bucket_width + 1)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 4.f + 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 4.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthFloatLessThan) {
  auto hist = EqualWidthHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 3u);

  const auto bucket_width = std::nextafter(6.1f - 0.5f, 6.1f - 0.5f + 1) / 3;

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1)},
                              PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1)},
                              PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{4.4f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan), (1.0f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan), (1.7f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1),
                                            PredicateCondition::LessThan),
                  4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::LessThan),
                  4.f + (2.5f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  4.f + (3.0f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  4.f + (3.3f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan),
                  4.f + (3.6f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  4.f + (3.9f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1),
                                            PredicateCondition::LessThan),
                  4.f + 7.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::LessThan),
                  4.f + 7.f + (4.4f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  4.f + 7.f + (5.9f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  4.f + 7.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthStringLessThan) {
  auto hist = EqualWidthHistogram<std::string>(_string3, "abcdefghijklmnopqrstuvwxyz", 4u);
  hist.generate(ColumnID{0}, 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "ghbp"
  constexpr auto bucket_1_upper = 6 * 26 * 26 * 26 + 7 * 26 * 26 + 1 * 26 + 15;
  // "ghbq"
  constexpr auto bucket_2_lower = bucket_1_upper + 1;
  // "mnbb"
  constexpr auto bucket_2_upper = 12 * 26 * 26 * 26 + 13 * 26 * 26 + 1 * 26 + 1;
  // "mnbc"
  constexpr auto bucket_3_lower = bucket_2_upper + 1;
  // "stan"
  constexpr auto bucket_3_upper = 18 * 26 * 26 * 26 + 19 * 26 * 26 + 0 * 26 + 13;
  // "stao"
  constexpr auto bucket_4_lower = bucket_3_upper + 1;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  constexpr auto bucket_1_count = 4.f;
  constexpr auto bucket_2_count = 5.f;
  constexpr auto bucket_3_count = 4.f;
  constexpr auto bucket_4_count = 3.f;
  constexpr auto total_count = bucket_1_count + bucket_2_count + bucket_3_count + bucket_4_count;

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aaaa", PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcd", PredicateCondition::LessThan), 0.f);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abce", PredicateCondition::LessThan), 1 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcf", PredicateCondition::LessThan), 2 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("cccc", PredicateCondition::LessThan),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("dddd", PredicateCondition::LessThan),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ghbo", PredicateCondition::LessThan),
                  (bucket_1_width - 2) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ghbp", PredicateCondition::LessThan),
                  (bucket_1_width - 1) / bucket_1_width * bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ghbq", PredicateCondition::LessThan), bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ghbr", PredicateCondition::LessThan),
                  1 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ghbs", PredicateCondition::LessThan),
                  2 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("jjjj", PredicateCondition::LessThan),
                  (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkk", PredicateCondition::LessThan),
                  (10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("lzzz", PredicateCondition::LessThan),
                  (11 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnaz", PredicateCondition::LessThan),
                  (bucket_2_width - 3) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnba", PredicateCondition::LessThan),
                  (bucket_2_width - 2) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnbb", PredicateCondition::LessThan),
                  (bucket_2_width - 1) / bucket_2_width * bucket_2_count + bucket_1_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnbc", PredicateCondition::LessThan), bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnbd", PredicateCondition::LessThan),
                  1 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("mnbe", PredicateCondition::LessThan),
                  2 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("pppp", PredicateCondition::LessThan),
                  (15 * 26 * 26 * 26 + 15 * 26 * 26 + 15 * 26 + 15 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qqqq", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qllo", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("stam", PredicateCondition::LessThan),
                  (bucket_3_width - 2) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("stan", PredicateCondition::LessThan),
                  (bucket_3_width - 1) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("stao", PredicateCondition::LessThan),
                  bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("stap", PredicateCondition::LessThan),
                  1 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("staq", PredicateCondition::LessThan),
                  2 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("vvvv", PredicateCondition::LessThan),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("xxxx", PredicateCondition::LessThan),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ycip", PredicateCondition::LessThan),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("yyzy", PredicateCondition::LessThan),
      (bucket_4_width - 2) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("yyzz", PredicateCondition::LessThan),
      (bucket_4_width - 1) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yzaa", PredicateCondition::LessThan), total_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzzz", PredicateCondition::LessThan), total_count);
}

TEST_F(HistogramTest, EqualHeightHistogramBasic) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1);
  hist.generate(ColumnID{1}, 4u);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(8, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 6 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightHistogramUnevenBuckets) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1);
  hist.generate(ColumnID{1}, 5u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(8, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(19, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightFloat) {
  auto hist = EqualHeightHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.1f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.2f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.5f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.5f, PredicateCondition::Equals), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightLessThan) {
  auto hist = EqualHeightHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (12'345 - 12 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan),
                  (1'234.f - 12) / (12'345 - 12 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan), 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(80'000, PredicateCondition::LessThan),
                  3.f + (80'000.f - 12'346) / (123'456 - 12'346 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan),
                  3.f + (123'456.f - 12'346) / (123'456 - 12'346 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 3.f + 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 3.f + 3.f);
}

TEST_F(HistogramTest, EqualHeightFloatLessThan) {
  auto hist = EqualHeightHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.2f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(2.5f, 2.5f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(4.4f, 4.4f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan), (1.0f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan), (1.7f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::LessThan), (2.2f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(2.5f, 2.5f + 1), PredicateCondition::LessThan), 5.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  5.f + (3.0f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  5.f + (3.3f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan),
                  5.f + (3.6f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  5.f + (3.9f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(4.4f, 4.4f + 1), PredicateCondition::LessThan), 5.f + 5.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  5.f + 5.f + (5.9f - (std::nextafter(4.4f, 4.4f + 1))) / (6.1f - std::nextafter(4.4f, 4.4f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  5.f + 5.f + 5.f);
}

TEST_F(HistogramTest, EqualHeightStringLessThan) {
  auto hist = EqualHeightHistogram<std::string>(_string3, "abcdefghijklmnopqrstuvwxyz", 4u);
  hist.generate(ColumnID{0}, 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "efgh"
  constexpr auto bucket_1_upper = 4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7;
  // "efgi"
  constexpr auto bucket_2_lower = bucket_1_upper + 1;
  // "kkkk"
  constexpr auto bucket_2_upper = 10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10;
  // "kkkl"
  constexpr auto bucket_3_lower = bucket_2_upper + 1;
  // "qrst"
  constexpr auto bucket_3_upper = 16 * 26 * 26 * 26 + 17 * 26 * 26 + 18 * 26 + 19;
  // "qrsu"
  constexpr auto bucket_4_lower = bucket_3_upper + 1;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  // Note that this is not the actual count in each bucket, but an approximation due to the type of the histogram.
  constexpr auto bucket_count = 4.f;
  constexpr auto total_count = 4 * bucket_count;

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aaaa", PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcd", PredicateCondition::LessThan), 0.f);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abce", PredicateCondition::LessThan), 1 / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcf", PredicateCondition::LessThan), 2 / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("cccc", PredicateCondition::LessThan),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("dddd", PredicateCondition::LessThan),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgg", PredicateCondition::LessThan),
                  (bucket_1_width - 2) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgh", PredicateCondition::LessThan),
                  (bucket_1_width - 1) / bucket_1_width * bucket_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgi", PredicateCondition::LessThan), bucket_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgj", PredicateCondition::LessThan),
                  1 / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgk", PredicateCondition::LessThan),
                  2 / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("ijkn", PredicateCondition::LessThan),
      (8 * 26 * 26 * 26 + 9 * 26 * 26 + 10 * 26 + 13 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("jjjj", PredicateCondition::LessThan),
      (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist.estimate_cardinality("jzzz", PredicateCondition::LessThan),
      (9 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kaab", PredicateCondition::LessThan),
                  (10 * 26 * 26 * 26 + 1 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkj", PredicateCondition::LessThan),
                  (bucket_2_width - 2) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkk", PredicateCondition::LessThan),
                  (bucket_2_width - 1) / bucket_2_width * bucket_count + bucket_count);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkl", PredicateCondition::LessThan), bucket_count * 2);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkm", PredicateCondition::LessThan),
                  1 / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("kkkn", PredicateCondition::LessThan),
                  2 / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("loos", PredicateCondition::LessThan),
                  (11 * 26 * 26 * 26 + 14 * 26 * 26 + 14 * 26 + 18 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("nnnn", PredicateCondition::LessThan),
                  (13 * 26 * 26 * 26 + 13 * 26 * 26 + 13 * 26 + 13 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qqqq", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qllo", PredicateCondition::LessThan),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrss", PredicateCondition::LessThan),
                  (bucket_3_width - 2) / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrst", PredicateCondition::LessThan),
                  (bucket_3_width - 1) / bucket_3_width * bucket_count + bucket_count * 2);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrsu", PredicateCondition::LessThan), bucket_count * 3);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrsv", PredicateCondition::LessThan),
                  1 / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("qrsw", PredicateCondition::LessThan),
                  2 / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("tdzr", PredicateCondition::LessThan),
                  (19 * 26 * 26 * 26 + 3 * 26 * 26 + 25 * 26 + 17 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("vvvv", PredicateCondition::LessThan),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("xxxx", PredicateCondition::LessThan),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ycip", PredicateCondition::LessThan),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yyzy", PredicateCondition::LessThan),
                  (bucket_4_width - 2) / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yyzz", PredicateCondition::LessThan),
                  (bucket_4_width - 1) / bucket_4_width * bucket_count + bucket_count * 3);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yzaa", PredicateCondition::LessThan), total_count);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzzz", PredicateCondition::LessThan), total_count);
}

TEST_F(HistogramTest, StringConstructorTests) {
  EXPECT_NO_THROW(EqualNumElementsHistogram<std::string>(_string2, "abcdefghijklmnopqrstuvwxyz", 13u));
  EXPECT_THROW(EqualNumElementsHistogram<std::string>(_string2, "abcdefghijklmnopqrstuvwxyz", 14u), std::exception);

  auto hist = EqualNumElementsHistogram<std::string>(_string2, "zyxwvutsrqponmlkjihgfedcba");
  EXPECT_EQ(hist.supported_characters(), "abcdefghijklmnopqrstuvwxyz");

  auto hist2 = EqualNumElementsHistogram<std::string>(_string2);
  EXPECT_EQ(hist2.supported_characters(), "abcdefghijklmnopqrstuvwxyz");
}

TEST_F(HistogramTest, EstimateCardinalityUnsupportedCharacters) {
  auto hist = EqualNumElementsHistogram<std::string>(_string2);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_NO_THROW(hist.estimate_cardinality("abcd", PredicateCondition::Equals));
  EXPECT_THROW(hist.estimate_cardinality("abc1", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("Abc", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("@", PredicateCondition::Equals), std::exception);
}

class HistogramPrivateTest : public BaseTest {
  void SetUp() override {
    const auto _string2 = load_table("src/test/tables/string2.tbl");
    _hist = std::make_shared<EqualNumElementsHistogram<std::string>>(_string2, "abcdefghijklmnopqrstuvwxyz", 4u);
    _hist->generate(ColumnID{0}, 2u);
  }

 protected:
  std::string previous_value(const std::string& value) { return _hist->_previous_value(value); }

  std::string next_value(const std::string& value) { return _hist->_next_value(value); }

  uint64_t convert_string_to_number_representation(const std::string& value) {
    return _hist->_convert_string_to_number_representation(value);
  }

  std::string convert_number_representation_to_string(const uint64_t value) {
    return _hist->_convert_number_representation_to_string(value);
  }

 protected:
  std::shared_ptr<EqualNumElementsHistogram<std::string>> _hist;
};

TEST_F(HistogramPrivateTest, PreviousValueString) {
  EXPECT_EQ(previous_value(""), "");
  EXPECT_EQ(previous_value("a"), "");
  EXPECT_EQ(previous_value("aaa"), "aa");
  EXPECT_EQ(previous_value("abcd"), "abcc");
  EXPECT_EQ(previous_value("abzz"), "abzy");
  EXPECT_EQ(previous_value("abca"), "abc");
  EXPECT_EQ(previous_value("abaa"), "aba");
  EXPECT_EQ(previous_value("aba"), "ab");
}

TEST_F(HistogramPrivateTest, NextValueString) {
  EXPECT_EQ(next_value(""), "a");
  EXPECT_EQ(next_value("abcd"), "abce");
  EXPECT_EQ(next_value("abaz"), "abba");
  EXPECT_EQ(next_value("abzz"), "acaa");
  EXPECT_EQ(next_value("abca"), "abcb");
  EXPECT_EQ(next_value("abaa"), "abab");
  EXPECT_EQ(next_value("zzzz"), "zzzza");
}

TEST_F(HistogramPrivateTest, StringToNumber) {
  EXPECT_EQ(convert_string_to_number_representation("aaaa"), 0ul);
  EXPECT_EQ(convert_string_to_number_representation("aaab"), 1ul);
  EXPECT_EQ(convert_string_to_number_representation("bhja"), 26ul * 26ul * 26ul + 7ul * 26ul * 26ul + 9ul * 26ul);
  EXPECT_EQ(convert_string_to_number_representation("zzzz"), 26ul * 26ul * 26ul * 26ul - 1ul);

  EXPECT_EQ(convert_string_to_number_representation("aaaa"), convert_string_to_number_representation("a"));
  EXPECT_EQ(convert_string_to_number_representation("dcba"), convert_string_to_number_representation("dcb"));
  EXPECT_NE(convert_string_to_number_representation("abcd"), convert_string_to_number_representation("bcd"));
}

TEST_F(HistogramPrivateTest, NumberToString) {
  EXPECT_EQ(convert_number_representation_to_string(0ul), "aaaa");
  EXPECT_EQ(convert_number_representation_to_string(1ul), "aaab");
  EXPECT_EQ(convert_number_representation_to_string(26ul * 26ul * 26ul + 7ul * 26ul * 26ul + 9ul * 26ul), "bhja");
  EXPECT_EQ(convert_number_representation_to_string(26ul * 26ul * 26ul * 26ul - 1ul), "zzzz");
}

// TODO(tim): remove
TEST_F(HistogramTest, StringComparisonTest) {
  EXPECT_TRUE(std::string("abcd") < std::string("abce"));
  EXPECT_TRUE(std::string("abc") < std::string("abca"));

  EXPECT_TRUE(std::string("Z") < std::string("a"));
  EXPECT_FALSE(std::string("azaaaaaaa") < std::string("aza"));
  EXPECT_TRUE(std::string("aZaaaaaaa") < std::string("aza"));
}

}  // namespace opossum