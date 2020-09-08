/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "generate_benchmark_input.hpp"
#include "random_distribution_factory.hpp"

#include <cudf/column/column.hpp>
#include <cudf/table/table.hpp>

#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/column_wrapper.hpp>

#include <rmm/device_buffer.hpp>

#include <future>
#include <memory>
#include <random>
#include <thread>
#include <vector>

/**
 * @brief Helper for pinned host memory
 **/
template <typename T>
using pinned_buffer = std::unique_ptr<T[], decltype(&cudaFreeHost)>;
template <typename T>
auto pinned_alloc = [](size_t count) {
  T* ptr = nullptr;
  CUDA_TRY(cudaMallocHost(&ptr, count * sizeof(T)));
  return ptr;
};

/**
 * @file generate_benchmark_input.hpp
 * @brief Contains functions that generate columns filled with random data.
 *
 * Also includes utilies that generate random tables.
 *
 * The distribution of random data is meant to simulate real-world data. For example, numerical
 * values are generated using a normal distribution with a zero mean. Therefore, different column
 * types are filled using different distributions. The distributions are documented in the
 * functions where they are used.
 *
 * Currently, the data generation is done on the CPU and the data is then copied to the device
 * memory.
 */

/**
 * @brief Mersenne Twister engine with static seed.
 *
 * Produces the same random sequence on each run.
 */
auto deterministic_engine(unsigned seed = 13377331) { return std::mt19937{seed}; }

template <typename T>
std::enable_if_t<cudf::is_fixed_width<T>(), size_t> avg_element_size(data_profile const& profile)
{
  return sizeof(T);
}

template <typename T>
std::enable_if_t<!cudf::is_fixed_width<T>(), size_t> avg_element_size(data_profile const& profile)
{
  CUDF_FAIL("not implemented!");
}

template <typename T>
T get_distribution_mean(distribution_params<T> const& dist)
{
  if (dist.id == distribution_id::NORMAL || dist.id == distribution_id::UNIFORM) {
    return dist.lower_bound / 2. + dist.upper_bound / 2.;
  }
  if (dist.id == distribution_id::GEOMETRIC) {
    auto const range_size = dist.lower_bound < dist.upper_bound
                              ? dist.upper_bound - dist.lower_bound
                              : dist.lower_bound - dist.upper_bound;
    auto const p = geometric_dist_p(range_size);
    if (dist.lower_bound < dist.upper_bound)
      return dist.lower_bound + 1. / p;
    else
      return dist.lower_bound - 1. / p;
  }
}

template <>
size_t avg_element_size<cudf::string_view>(data_profile const& profile)
{
  auto const dist = profile.get_distribution_params<cudf::string_view>().length_params;
  return get_distribution_mean(dist);
}

template <>
size_t avg_element_size<cudf::list_view>(data_profile const& profile)
{
  auto const dist_params       = profile.get_distribution_params<cudf::list_view>();
  auto const single_level_mean = get_distribution_mean(dist_params.length_params);
  auto const element_size      = cudf::size_of(cudf::data_type{dist_params.element_type});
  return element_size * pow(single_level_mean, dist_params.max_depth);
}

struct avg_element_size_fn {
  template <typename T>
  size_t operator()(data_profile const& profile)
  {
    return avg_element_size<T>(profile);
  }
};

size_t avg_element_bytes(data_profile const& profile, cudf::type_id tid)
{
  return cudf::type_dispatcher(cudf::data_type(tid), avg_element_size_fn{}, profile);
}
template <typename T, typename Enable = void>
struct random_value_fn;

/**
 * @brief nanosecond count in the unit of @ref T.
 *
 * @tparam T Timestamp type
 */
template <typename T>
constexpr int64_t to_nanoseconds(int64_t t)
{
  using ratio = std::ratio_divide<typename T::period, typename cudf::timestamp_ns::period>;
  return t * ratio::num / ratio::den;
}

/**
 * @brief Creates an random timestamp
 *
 * Generates 'recent' timestamps. All timstamps are earlier that June 2020. The period between the
 * timestamps and June 2020 has a geometric distribution. Most timestamps are within a few years
 * before 2020.
 *
 * @return The random timestamp
 * @tparam T Timestamp type
 */
template <typename T>
struct random_value_fn<T, typename std::enable_if_t<cudf::is_chrono<T>()>> {
  std::function<int64_t(std::mt19937&)> seconds_gen;
  std::function<int64_t(std::mt19937&)> nanoseconds_gen;

  random_value_fn(distribution_params<T> params)
  {
    std::pair<int64_t, int64_t> const range_ns = {to_nanoseconds<T>(params.lower_bound),
                                                  to_nanoseconds<T>(params.upper_bound)};
    std::pair<int64_t, int64_t> const range_s  = {
      range_ns.first / to_nanoseconds<cudf::timestamp_s>(1),
      range_ns.second / to_nanoseconds<cudf::timestamp_s>(1)};
    if (range_ns.first != range_ns.second)
      seconds_gen = make_distribution<int64_t>(params.id, range_s.first, range_s.second);
    else
      seconds_gen = [=](std::mt19937&) { return range_ns.second; };

    auto const range_size_ns = range_ns.second - range_ns.first;
    // Don't need nanoseconds for days or seconds
    if (to_nanoseconds<T>(1) < 1000000000)
      nanoseconds_gen = make_distribution<int64_t>(
        distribution_id::UNIFORM, std::min(range_size_ns, 0l), std::max(range_size_ns, 0l));
    else
      nanoseconds_gen = [](std::mt19937&) { return 0; };
  }

  T operator()(std::mt19937& engine)
  {
    // Subtract the seconds from the 2020 timestamp to generate a reccent timestamp
    auto const timestamp_ns =
      to_nanoseconds<cudf::timestamp_s>(seconds_gen(engine)) + nanoseconds_gen(engine);
    // Return value in the type's precision
    return T(typename T::duration{timestamp_ns / to_nanoseconds<T>(1)});
  }
};

template <typename T>
struct random_value_fn<T, typename std::enable_if_t<cudf::is_fixed_point<T>()>> {
  random_value_fn(distribution_params<T> const&) {}
  T operator()(std::mt19937& engine) { return T{}; }
};

template <typename T>
struct random_value_fn<
  T,
  typename std::enable_if_t<!std::is_same<T, bool>::value && cudf::is_numeric<T>()>> {
  T const lower_bound;
  T const upper_bound;
  distribution_fn<T> dist;

  random_value_fn(distribution_params<T> const& desc)
    : lower_bound{desc.lower_bound},
      upper_bound{desc.upper_bound},
      dist{make_distribution<T>(desc.id, desc.lower_bound, desc.upper_bound)}
  {
  }

  T operator()(std::mt19937& engine)
  {
    return std::max(std::min(dist(engine), upper_bound), lower_bound);
  }
};

/**
 * @brief Creates an boolean value with given probability of returning `true`.
 *
 * @return The random boolean value
 */
template <typename T>
struct random_value_fn<T, typename std::enable_if_t<std::is_same<T, bool>::value>> {
  std::bernoulli_distribution b_dist;

  random_value_fn(distribution_params<T> const& desc) : b_dist{desc.probability_true} {}
  T operator()(std::mt19937& engine) { return b_dist(engine); }
};

size_t constexpr bitmask_bits = sizeof(cudf::bitmask_type) * 8;
size_t null_mask_size(cudf::size_type num_rows)
{
  return (num_rows + bitmask_bits - 1) / bitmask_bits;
}

bool get_null_mask_bit(std::vector<cudf::bitmask_type> const& null_mask_data, cudf::size_type row)
{
  return null_mask_data[row / bitmask_bits] & (cudf::bitmask_type(1) << row % bitmask_bits);
}

void reset_null_mask_bit(std::vector<cudf::bitmask_type>& null_mask_data, cudf::size_type row)
{
  null_mask_data[row / bitmask_bits] &= ~(cudf::bitmask_type(1) << row % bitmask_bits);
}

template <typename T>
void set_element_at(
  T value, bool valid, T* values, std::vector<cudf::bitmask_type>& null_mask, cudf::size_type idx)
{
  if (valid) {
    values[idx] = value;
  } else {
    reset_null_mask_bit(null_mask, idx);
  }
}

/**
 * @brief Creates a column with random content of the given type
 *
 * The templated implementation is used for all fixed width types. String columns are generated
 * using the specialization implemented below.
 *
 * @param[in] TODO
 *
 * @return Column filled with random data
 */
template <typename T>
std::unique_ptr<cudf::column> create_random_column(data_profile const& profile,
                                                   std::mt19937& engine,
                                                   cudf::size_type num_rows)
{
  auto valid_dist = std::bernoulli_distribution{1. - profile.get_null_frequency()};
  auto value_dist = random_value_fn<T>{profile.get_distribution_params<T>()};

  auto const cardinality = std::min(num_rows, profile.get_cardinality());
  pinned_buffer<T> samples{pinned_alloc<T>(cardinality), cudaFreeHost};
  std::vector<cudf::bitmask_type> samples_null_mask(null_mask_size(cardinality), ~0);
  for (cudf::size_type si = 0; si < cardinality; ++si) {
    set_element_at(value_dist(engine), valid_dist(engine), samples.get(), samples_null_mask, si);
  }

  std::uniform_int_distribution<cudf::size_type> sample_dist{0, cardinality - 1};
  auto const avg_run_len = profile.get_avg_run_length();
  std::gamma_distribution<float> run_len_dist(4.f, avg_run_len / 4.f);
  pinned_buffer<T> data{pinned_alloc<T>(num_rows), cudaFreeHost};
  std::vector<cudf::bitmask_type> null_mask(null_mask_size(num_rows), ~0);

  for (cudf::size_type row = 0; row < num_rows; ++row) {
    if (cardinality == 0) {
      set_element_at(value_dist(engine), valid_dist(engine), data.get(), null_mask, row);
    } else {
      auto const sample_idx = sample_dist(engine);
      set_element_at(samples[sample_idx],
                     get_null_mask_bit(samples_null_mask, sample_idx),
                     data.get(),
                     null_mask,
                     row);
    }

    if (avg_run_len > 1) {
      int const run_len = std::min<int>(num_rows - row, std::round(run_len_dist(engine)));
      for (int offset = 1; offset < run_len; ++offset) {
        set_element_at(
          data[row], get_null_mask_bit(null_mask, row), data.get(), null_mask, row + offset);
      }
      row += std::max(run_len - 1, 0);
    }
  }

  return std::make_unique<cudf::column>(
    cudf::data_type{cudf::type_to_id<T>()},
    num_rows,
    rmm::device_buffer(data.get(), num_rows * sizeof(T), cudaStream_t(0)),
    rmm::device_buffer(
      null_mask.data(), null_mask.size() * sizeof(cudf::bitmask_type), cudaStream_t(0)));
}

struct string_col_data {
  std::vector<char> chars;
  std::vector<int32_t> offsets;
  std::vector<cudf::bitmask_type> null_mask;
  explicit string_col_data(cudf::size_type rows, cudf::size_type size)
  {
    offsets.reserve(rows + 1);
    offsets.push_back(0);
    chars.reserve(size);
    null_mask.insert(null_mask.end(), null_mask_size(rows), ~0);
  }
};

// Assumes that the null mask is initialized with all bits valid
void copy_string(cudf::size_type src_idx,
                 string_col_data const& src,
                 cudf::size_type dst_idx,
                 string_col_data& dst)
{
  if (!get_null_mask_bit(src.null_mask, src_idx)) reset_null_mask_bit(dst.null_mask, dst_idx);
  auto const str_len = src.offsets[src_idx + 1] - src.offsets[src_idx];
  dst.chars.resize(dst.chars.size() + str_len);
  if (get_null_mask_bit(src.null_mask, src_idx)) {
    std::copy_n(
      src.chars.begin() + src.offsets[src_idx], str_len, dst.chars.begin() + dst.offsets.back());
  }
  dst.offsets.push_back(dst.chars.size());
}

template <typename Char_gen>
void append_string(Char_gen& char_gen, bool valid, uint32_t length, string_col_data& column_data)
{
  auto const idx = column_data.offsets.size() - 1;
  column_data.offsets.push_back(column_data.offsets.back() + length);
  std::generate_n(std::back_inserter(column_data.chars),
                  column_data.offsets[idx + 1] - column_data.offsets[idx],
                  [&]() { return char_gen(); });

  // TODO: use empty string for invalid fields?
  if (!valid) { reset_null_mask_bit(column_data.null_mask, idx); }
}

/**
 * @brief Creates a string column with random content
 *
 * Due to random generation of the length of the columns elements, the resulting column will
 * have a slightly different size from @ref col_bytes.
 *
 * @param[in] TODO
 *
 * @return Column filled with random data
 */
template <>
std::unique_ptr<cudf::column> create_random_column<cudf::string_view>(data_profile const& profile,
                                                                      std::mt19937& engine,
                                                                      cudf::size_type num_rows)
{
  auto char_dist = [&engine, dist = std::uniform_int_distribution<char>{'!', '~'}]() mutable {
    return dist(engine);
  };
  auto len_dist =
    random_value_fn<uint32_t>{profile.get_distribution_params<cudf::string_view>().length_params};
  auto valid_dist = std::bernoulli_distribution{1. - profile.get_null_frequency()};

  auto const avg_string_len = avg_element_size<cudf::string_view>(profile);
  auto const cardinality    = std::min(profile.get_cardinality(), num_rows);
  string_col_data samples(cardinality, cardinality * avg_string_len);
  for (cudf::size_type si = 0; si < cardinality; ++si) {
    append_string(char_dist, valid_dist(engine), len_dist(engine), samples);
  }

  auto const avg_run_len = profile.get_avg_run_length();
  std::gamma_distribution<float> run_len_dist(4.f, avg_run_len / 4.f);

  string_col_data out_col(num_rows, num_rows * avg_string_len);
  std::uniform_int_distribution<cudf::size_type> sample_dist{0, cardinality - 1};
  for (cudf::size_type row = 0; row < num_rows; ++row) {
    if (cardinality == 0) {
      append_string(char_dist, valid_dist(engine), len_dist(engine), out_col);
    } else {
      copy_string(sample_dist(engine), samples, row, out_col);
    }
    if (avg_run_len > 1) {
      int const run_len = std::min<int>(num_rows - row, std::round(run_len_dist(engine)));
      for (int offset = 1; offset < run_len; ++offset) {
        copy_string(row, out_col, row + offset, out_col);
      }
      row += std::max(run_len - 1, 0);
    }
  }
  return cudf::make_strings_column(out_col.chars, out_col.offsets, out_col.null_mask);
}

template <>
std::unique_ptr<cudf::column> create_random_column<cudf::dictionary32>(data_profile const& profile,
                                                                       std::mt19937& engine,
                                                                       cudf::size_type num_rows)
{
  CUDF_FAIL("not implemented yet");
}

template <>
std::unique_ptr<cudf::column> create_random_column<cudf::struct_view>(data_profile const& profile,
                                                                      std::mt19937& engine,
                                                                      cudf::size_type num_rows)
{
  CUDF_FAIL("not implemented yet");
}

struct create_rand_col_fn {
 public:
  template <typename T>
  std::unique_ptr<cudf::column> operator()(data_profile const& profile,
                                           std::mt19937& engine,
                                           cudf::size_type num_rows)
  {
    return create_random_column<T>(profile, engine, num_rows);
  }
};

template <>
std::unique_ptr<cudf::column> create_random_column<cudf::list_view>(data_profile const& profile,
                                                                    std::mt19937& engine,
                                                                    cudf::size_type num_rows)
{
  auto const dist_params       = profile.get_distribution_params<cudf::list_view>();
  auto const single_level_mean = get_distribution_mean(dist_params.length_params);
  auto const num_elements      = num_rows * pow(single_level_mean, dist_params.max_depth);

  auto child_column = cudf::type_dispatcher(
    cudf::data_type(dist_params.element_type), create_rand_col_fn{}, profile, engine, num_elements);
  auto len_dist =
    random_value_fn<uint32_t>{profile.get_distribution_params<cudf::list_view>().length_params};
  auto valid_dist = std::bernoulli_distribution{1. - profile.get_null_frequency()};

  for (int lvl = 0; lvl < dist_params.max_depth; ++lvl) {
    auto const num_rows = child_column->size() / single_level_mean;

    std::vector<int32_t> offsets{0};
    offsets.reserve(num_rows + 1);
    std::vector<cudf::bitmask_type> null_mask(null_mask_size(num_rows), ~0);
    for (int row = 1; row < num_rows + 1; ++row) {
      offsets.push_back(std::min<int32_t>(child_column->size(), offsets.back() + len_dist(engine)));
      if (!valid_dist(engine)) reset_null_mask_bit(null_mask, row);
    }
    offsets.back() = child_column->size();  // Always include all elements

    auto offsets_column = std::make_unique<cudf::column>(
      cudf::data_type{cudf::type_id::INT32},
      offsets.size(),
      rmm::device_buffer(offsets.data(), offsets.size() * sizeof(int32_t), cudaStream_t(0)));

    auto list_column = cudf::make_lists_column(
      num_rows,
      std::move(offsets_column),
      std::move(child_column),
      cudf::UNKNOWN_NULL_COUNT,
      rmm::device_buffer(
        null_mask.data(), null_mask.size() * sizeof(cudf::bitmask_type), cudaStream_t(0)));
    child_column = std::move(list_column);
  }
  return child_column;
}

using columns_vector = std::vector<std::unique_ptr<cudf::column>>;

columns_vector create_random_columns(data_profile const& profile,
                                     std::vector<cudf::type_id> dtype_ids,
                                     std::mt19937 engine,
                                     cudf::size_type num_rows)
{
  columns_vector output_columns;
  std::transform(
    dtype_ids.begin(), dtype_ids.end(), std::back_inserter(output_columns), [&](auto tid) {
      return cudf::type_dispatcher(
        cudf::data_type(tid), create_rand_col_fn{}, profile, engine, num_rows);
    });
  return output_columns;
}

std::vector<cudf::type_id> repeat_dtypes(std::vector<cudf::type_id> const& dtype_ids,
                                         cudf::size_type num_cols)
{
  if (dtype_ids.size() == num_cols) { return dtype_ids; }
  std::vector<cudf::type_id> out_dtypes;
  out_dtypes.reserve(num_cols);
  for (cudf::size_type col = 0; col < num_cols; ++col)
    out_dtypes.push_back(dtype_ids[col % dtype_ids.size()]);
  return out_dtypes;
}

std::unique_ptr<cudf::table> create_random_table(std::vector<cudf::type_id> const& dtype_ids,
                                                 cudf::size_type num_cols,
                                                 table_size_bytes table_bytes,
                                                 data_profile const& profile,
                                                 unsigned seed)
{
  auto const out_dtype_ids = repeat_dtypes(dtype_ids, num_cols);
  size_t const avg_row_bytes =
    std::accumulate(out_dtype_ids.begin(), out_dtype_ids.end(), 0ul, [&](size_t sum, auto tid) {
      return sum + avg_element_bytes(profile, tid);
    });
  cudf::size_type const num_rows = table_bytes.size / avg_row_bytes;

  return create_random_table(out_dtype_ids, num_cols, row_count{num_rows}, profile, seed);
}

std::unique_ptr<cudf::table> create_random_table(std::vector<cudf::type_id> const& dtype_ids,
                                                 cudf::size_type num_cols,
                                                 row_count num_rows,
                                                 data_profile const& profile,
                                                 unsigned seed)
{
  auto const out_dtype_ids = repeat_dtypes(dtype_ids, num_cols);
  auto seed_engine         = deterministic_engine(seed);

  auto const processor_count            = std::thread::hardware_concurrency();
  cudf::size_type const cols_per_thread = (num_cols + processor_count - 1) / processor_count;
  cudf::size_type next_col              = 0;
  std::vector<std::future<columns_vector>> col_futures;
  random_value_fn<unsigned> seed_dist(
    {distribution_id::UNIFORM, 0, std::numeric_limits<unsigned>::max()});
  for (unsigned int i = 0; i < processor_count && next_col < num_cols; ++i) {
    auto thread_engine         = deterministic_engine(seed_dist(seed_engine));
    auto const thread_num_cols = std::min(num_cols - next_col, cols_per_thread);
    std::vector<cudf::type_id> thread_types(out_dtype_ids.begin() + next_col,
                                            out_dtype_ids.begin() + next_col + thread_num_cols);
    col_futures.emplace_back(std::async(std::launch::async,
                                        create_random_columns,
                                        std::cref(profile),
                                        std::move(thread_types),
                                        std::move(thread_engine),
                                        num_rows.count));
    next_col += thread_num_cols;
  }

  columns_vector output_columns;
  for (auto& cf : col_futures) {
    auto partial_table = cf.get();
    output_columns.reserve(output_columns.size() + partial_table.size());
    std::move(
      std::begin(partial_table), std::end(partial_table), std::back_inserter(output_columns));
    partial_table.clear();
  }

  return std::make_unique<cudf::table>(std::move(output_columns));
}

std::vector<cudf::type_id> get_type_or_group(int32_t id)
{
  // identity transformation when passing a concrete type_id
  if (id < static_cast<int32_t>(cudf::type_id::NUM_TYPE_IDS))
    return {static_cast<cudf::type_id>(id)};

  // if the value is larger that type_id::NUM_TYPE_IDS, it's a group id
  type_group_id const group_id = static_cast<type_group_id>(id);

  using trait_fn       = bool (*)(cudf::data_type);
  trait_fn is_integral = [](cudf::data_type type) {
    return cudf::is_numeric(type) && !cudf::is_floating_point(type);
  };
  trait_fn is_integral_signed = [](cudf::data_type type) {
    return cudf::is_numeric(type) && !cudf::is_floating_point(type) && !cudf::is_unsigned(type);
  };
  auto fn = [&]() -> trait_fn {
    switch (group_id) {
      case type_group_id::FLOATING_POINT: return cudf::is_floating_point;
      case type_group_id::INTEGRAL: return is_integral;
      case type_group_id::INTEGRAL_SIGNED: return is_integral_signed;
      case type_group_id::NUMERIC: return cudf::is_numeric;
      case type_group_id::TIMESTAMP: return cudf::is_timestamp;
      case type_group_id::DURATION: return cudf::is_duration;
      case type_group_id::FIXED_POINT: return cudf::is_fixed_point;
      case type_group_id::COMPOUND: return cudf::is_compound;
      case type_group_id::NESTED: return cudf::is_nested;
      default: CUDF_FAIL("Invalid data type group");
    }
  }();
  std::vector<cudf::type_id> types;
  for (int type_int = 0; type_int < static_cast<int32_t>(cudf::type_id::NUM_TYPE_IDS); ++type_int) {
    auto const type = static_cast<cudf::type_id>(type_int);
    if (type != cudf::type_id::EMPTY && fn(cudf::data_type(type))) types.push_back(type);
  }
  return types;
}
