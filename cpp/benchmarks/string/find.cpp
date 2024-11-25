/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

#include <benchmarks/common/generate_input.hpp>
#include <benchmarks/fixture/benchmark_fixture.hpp>

#include <cudf_test/column_wrapper.hpp>

#include <cudf/scalar/scalar.hpp>
#include <cudf/strings/find.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/utilities/default_stream.hpp>

#include <nvbench/nvbench.cuh>

static void bench_find_string(nvbench::state& state)
{
  auto const n_rows    = static_cast<cudf::size_type>(state.get_int64("num_rows"));
  auto const row_width = static_cast<cudf::size_type>(state.get_int64("row_width"));
  auto const hit_rate  = static_cast<cudf::size_type>(state.get_int64("hit_rate"));
  auto const api       = state.get_string("api");

  if (static_cast<std::size_t>(n_rows) * static_cast<std::size_t>(row_width) >=
      static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())) {
    state.skip("Skip benchmarks greater than size_type limit");
  }

  auto const stream = cudf::get_default_stream();
  auto const col    = create_string_column(n_rows, row_width, hit_rate);
  auto const input  = cudf::strings_column_view(col->view());

  cudf::string_scalar target("0987 5W43");

  state.set_cuda_stream(nvbench::make_cuda_stream_view(stream.value()));
  auto const chars_size = input.chars_size(stream);
  state.add_element_count(chars_size, "chars_size");
  state.add_global_memory_reads<nvbench::int8_t>(chars_size);
  if (api == "find") {
    state.add_global_memory_writes<nvbench::int32_t>(input.size());
  } else {
    state.add_global_memory_writes<nvbench::int8_t>(input.size());
  }

  if (api == "find") {
    state.exec(nvbench::exec_tag::sync,
               [&](nvbench::launch& launch) { cudf::strings::find(input, target); });
  } else if (api == "contains") {
    constexpr bool combine          = false;  // test true/false
    bool has_same_target_first_char = false;  // test true/false
    constexpr int iters             = 10;     // test 4/10
    bool check_result               = false;

    std::vector<std::string> match_targets({" abc",
                                            "W43",
                                            "0987 5W43",
                                            "123 abc",
                                            "23 abc",
                                            "3 abc",
                                            "é",
                                            "7 5W43",
                                            "87 5W43",
                                            "987 5W43"});
    auto multi_targets = std::vector<std::string>{};
    for (int i = 0; i < iters; i++) {
      // if has same first chars in targets, use duplicated targets.
      int idx = has_same_target_first_char ? i / 2 : i;
      multi_targets.emplace_back(match_targets[idx]);
    }

    if constexpr (not combine) {
      state.exec(nvbench::exec_tag::sync, [&](nvbench::launch& launch) {
        std::vector<std::unique_ptr<cudf::column>> contains_results;
        std::vector<cudf::column_view> contains_cvs;
        for (size_t i = 0; i < multi_targets.size(); i++) {
          contains_results.emplace_back(
            cudf::strings::contains(input, cudf::string_scalar(multi_targets[i])));
          contains_cvs.emplace_back(contains_results.back()->view());
        }

        if (check_result) {
          cudf::test::strings_column_wrapper multi_targets_column(multi_targets.begin(),
                                                                  multi_targets.end());
          auto tab =
            cudf::strings::multi_contains(input, cudf::strings_column_view(multi_targets_column));
          for (int i = 0; i < tab->num_columns(); i++) {
            cudf::test::detail::expect_columns_equal(contains_cvs[i], tab->get_column(i).view());
          }
        }
      });
    } else {  // combine
      state.exec(nvbench::exec_tag::sync, [&](nvbench::launch& launch) {
        cudf::test::strings_column_wrapper multi_targets_column(multi_targets.begin(),
                                                                multi_targets.end());
        cudf::strings::multi_contains(input, cudf::strings_column_view(multi_targets_column));
      });
    }
  } else if (api == "starts_with") {
    state.exec(nvbench::exec_tag::sync,
               [&](nvbench::launch& launch) { cudf::strings::starts_with(input, target); });
  } else if (api == "ends_with") {
    state.exec(nvbench::exec_tag::sync,
               [&](nvbench::launch& launch) { cudf::strings::ends_with(input, target); });
  }
}

NVBENCH_BENCH(bench_find_string)
  .set_name("find_string")
  .add_string_axis("api", {"find", "contains", "starts_with", "ends_with"})
  .add_int64_axis("row_width", {32, 64, 128, 256, 512, 1024})
  .add_int64_axis("num_rows", {260'000, 1'953'000, 16'777'216})
  .add_int64_axis("hit_rate", {20, 80});  // percentage
