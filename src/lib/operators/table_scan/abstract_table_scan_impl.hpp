#pragma once

#include <array>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief the base class of all table scan impls
 */
class AbstractTableScanImpl {
 public:
  virtual ~AbstractTableScanImpl() = default;

  virtual std::string description() const = 0;

  virtual std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) const = 0;

 protected:
  /**
   * @defgroup The hot loop of the table scan
   * @{
   */

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator>
  void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, bool functor_is_vectorizable) const {
    // Can't use a default argument for this because default arguments are non-type deduced contexts
    auto false_type = std::false_type{};
    _scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, functor_is_vectorizable,
                                       false_type);
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, [[maybe_unused]] bool functor_is_vectorizable,
                       [[maybe_unused]] RightIterator& right_it) const {
    // SIMD has no benefit for iterators that are too complex (mostly iterators that do not operate on contiguous
    // storage). Currently, it is only enabled for std::vector (as used by FixedSizeByteAlignedVector). Also, the
    // AnySegmentIterator is not vectorizable because it relies on virtual method calls. While the check for `IS_DEBUG`
    // is redudant, it makes people aware of this.
    //
    // Unfortunately, vectorization is only really beneficial when we can use AVX-512VL. However, since the SIMD branch
    // is not slower on CPUs without AVX-512VL, we use it in any case. This reduces the divergence across different
    // systems. Finally, we only use the vectorized scan for tables with a certain size. This is because firing up the
    // AVX units has some cost on current CPUs. Using 1000 as the boundary is just an educated guess - a
    // machine-dependent fine-tuning could find better values, but as long as scans with a handful of results are not
    // vectorized, the benefits of fine-tuning should not be too big.
    //
    // See the SIMD method for a comment on IsVectorizable.

#if !IS_DEBUG
    if constexpr (LeftIterator::IsVectorizable) {
      if (functor_is_vectorizable && left_end - left_it > 1000) {
        _simd_scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, right_it);
      }
    }
#endif

    // Do the remainder the easy way. If we did not use the optimization above, left_it was not yet touched, so we
    // iterate over the entire input data.
    for (; left_it != left_end; ++left_it) {
      if constexpr (std::is_same_v<RightIterator, std::false_type>) {
        const auto left = *left_it;

        if ((!CheckForNull || !left.is_null()) && func(left)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
      } else {
        const auto left = *left_it;
        const auto right = *right_it;
        if ((!CheckForNull || (!left.is_null() && !right.is_null())) && func(left, right)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
        ++right_it;
      }
    }
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline))
  _simd_scan_with_iterators(const BinaryFunctor func, LeftIterator& left_it, const LeftIterator left_end,
                            const ChunkID chunk_id, PosList& matches_out,
                            [[maybe_unused]] RightIterator& right_it) const {
    // Concept: Partition the vector into blocks of BLOCK_SIZE entries. The remainder is handled outside of this
    // optimization. For each row, we write 0 to `offsets` if the row does not match, or `chunk_offset + 1` if the row
    // matches. The reason why we need `+1` is given below. This can be parallelized using auto-vectorization/SIMD.
    // Afterwards, we add all matching rows into `matches_out`. There, we do not push_back/emplace_back values, but
    // resize the vector first and directly write values into the next position, given by matches_out_index. This
    // avoids calls into the stdlib from the hot loop.

    auto matches_out_index = matches_out.size();
    constexpr size_t SIMD_SIZE = 64;  // Assuming a maximum SIMD register size of 512 bit
    constexpr size_t BLOCK_SIZE = SIMD_SIZE / sizeof(ValueID);

    // Continue the following until we have too few rows left to run over a whole block
    while (static_cast<size_t>(left_end - left_it) > BLOCK_SIZE) {
      alignas(SIMD_SIZE) std::array<ChunkOffset, SIMD_SIZE / sizeof(ChunkOffset)> offsets;

      // The pragmas promise to the compiler that there are no data dependencies within the loop. If you run into any
      // issues with the optimization, make sure that you only have only set IsVectorizable on iterators that use
      // linear storage and where the access methods do not change any state.
      //
      // Also, when using clang, this causes an error to be thrown if the loop could not be vectorized. Seeing no
      // error, however, just means that some part of the loop was vectorized - it does not mean that the loop has no
      // sequential components. For developing fast SIMD methods, you won't get around disassembling the respective
      // object file.
      //
      // Finally, a word on IsVectorizable: With the pragma giving the guarantee about no hidden dependencies, both
      // gcc and clang can identify cases where SIMD is beneficial. However, when `loop vectorize` is set for clang,
      // it throws errors for non-vectorized loops. To avoid these compiler errors, we don't even enter this method
      // if we know that a certain iterator or functor cannot be vectorized. Sometimes, clang also throws an error
      // even if the iterator itself is generally vectorizable. In that case, the functor (together with a potential
      // check for NULL values) is too complex. I (MD) have not yet fully understood at what point this happens. You
      // can avoid it by (a) reducing the number of instructions needed for your functor and (b) trying to make NULL
      // checks unnecessary.

      // NOLINTNEXTLINE
      ;  // clang-format off
      #pragma GCC ivdep
      #pragma clang loop vectorize(assume_safety)
      // clang-format on
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        const auto& left = *left_it;

        bool matches;
        if constexpr (std::is_same_v<RightIterator, std::false_type>) {
          matches = (!CheckForNull | !left.is_null()) & func(left);
        } else {
          const auto& right = *left_it;
          matches = (!CheckForNull | (!left.is_null() & !right.is_null())) & func(left, right);
        }

        // If the row matches, write its offset+1 into `offsets`, otherwise write 0. We need to increment the offset
        // because otherwise a zero would be written for both "no match" and "match at offset 0". This is safe because
        // the last possible chunk offset is defined as INVALID_CHUNK_OFFSET anyway. We later subtract from the offset
        // again.
        offsets[i] = matches * (left.chunk_offset() + 1);

        ++left_it;
        if constexpr (!std::is_same_v<RightIterator, std::false_type>) ++right_it;
      }

      // As we write directly into the matches_out vector, make sure that is has enough size
      if (matches_out_index + BLOCK_SIZE >= matches_out.size()) {
        matches_out.resize((BLOCK_SIZE + matches_out.size()) * 3, RowID{chunk_id, 0});
      }

      // Now write the matches into matches_out.
#ifndef __AVX512VL__
      // "Slow" path for non-AVX512VL systems
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        if (offsets[i]) {
          matches_out[matches_out_index++].chunk_offset = offsets[i] - 1;
        }
      }
#else
      // Fast path for AVX512VL systems

      // Build a mask where a set bit indicates that the row in `offsets` matched the criterion.
      const auto mask = _mm512_cmpneq_epu32_mask(*reinterpret_cast<__m512i*>(&offsets), __m512i{});

      if (!mask) continue;

      // Compress `offsets`, i.e., move all values where the mask is set to 1 to the front. This is essentially
      // std::remove(offsets.begin(), offsets.end(), ChunkOffset{0});
      *reinterpret_cast<__m512i*>(&offsets) = _mm512_maskz_compress_epi32(mask, *reinterpret_cast<__m512i*>(&offsets));

      // Copy all offsets into `matches_out` - even those that are set to 0. This does not matter because they will
      // be overwritten in the next round anyway. Copying more than necessary is better than stopping at the number
      // of matching rows because we do not need a branch for this. The loop will be vectorized automatically.
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        matches_out[matches_out_index + i].chunk_offset = offsets[i] - 1;
      }

      // Count the number of matches and increase the index of the next write to matches_out accordingly
      matches_out_index += __builtin_popcount(mask);
#endif
    }

    // Remove all entries that we have overallocated
    matches_out.resize(matches_out_index);

    // The remainder is now done by the regular scan
  }

  /**@}*/
};

}  // namespace opossum
