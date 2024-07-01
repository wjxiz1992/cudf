/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

#include "groupby/sort/group_single_pass_reduction_util.cuh"

#include <cudf/detail/gather.hpp>
#include <cudf/utilities/span.hpp>
#include <cudf_test/debug_utilities.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/resource_ref.hpp>

#include <thrust/gather.h>

namespace cudf {
namespace groupby {
namespace detail {
std::unique_ptr<column> group_min_by(column_view const& structs_column,
                                     column_view const& group_sizes,
                                     cudf::device_span<size_type const> group_labels,
                                     cudf::device_span<size_type const> group_offsets,
                                     size_type num_groups,
                                     column_view const& key_sort_order,
                                     rmm::cuda_stream_view stream,
                                     rmm::device_async_resource_ref mr)
{
  cudf::test::print(structs_column);
  printf("column type %d\n", int(structs_column.type().id()));
  // Extract the child columns from the structs column
  auto structs_view = cudf::structs_column_view{structs_column};
  auto const values       = cudf::structs_column_view{structs_view}.get_sliced_child(0);
  auto const orders     = cudf::structs_column_view{structs_view}.get_sliced_child(1);

  cudf::test::print(values);
  cudf::test::print(orders);

  printf("values type %d\n", int(values.type().id()));
  printf("orders type %d\n", int(orders.type().id()));

  // auto const n = 0;  // nth element to extract

  if (num_groups == 0) { return empty_like(values); }

  auto indices = type_dispatcher(values.type(),
                                 group_reduction_dispatcher<aggregation::ARGMIN>{},
                                 orders,
                                 num_groups,
                                 group_labels,
                                 stream,
                                 mr);

  printf("!!group_min_by flag 1\n");

  auto indices_view = indices->mutable_view();
  cudf::test::print(indices->view());

  // thrust::gather_if(rmm::exec_policy(stream),
  //                   indices_view.begin<size_type>(),    // map first
  //                   indices_view.end<size_type>(),      // map last
  //                   indices_view.begin<size_type>(),    // stencil
  //                   key_sort_order.begin<size_type>(),  // input
  //                   indices_view.begin<size_type>(),    // result
  //                   [] __device__(auto i) { return (i != cudf::detail::ARGMIN_SENTINEL); });

  // printf("!!! indices_view has nulls %d\n", int(indices_view.has_nulls()));

  auto groupby_order = cudf::detail::gather(table_view{{orders}},
                                           indices_view,
                                           out_of_bounds_policy::NULLIFY,
                                           cudf::detail::negative_index_policy::NOT_ALLOWED,
                                           stream,
                                           mr);

  printf("!!group_min_by flag 2\n");

  auto groupby_value = cudf::detail::gather(table_view{{values}},
                                          indices_view,
                                          out_of_bounds_policy::NULLIFY,
                                          cudf::detail::negative_index_policy::NOT_ALLOWED,
                                          stream,
                                          mr);

  printf("!!group_min_by flag 3\n");

  // construct the output table to a struct column with groupby_value and groupby_order
  auto groupby_order_column = groupby_order->get_column(0);
  auto groupby_value_column = groupby_value->get_column(0);

  printf("!!group_min_by flag 4\n");

  auto const num_rows = groupby_value_column.size();
  std::vector<std::unique_ptr<column>> output_columns;

  output_columns.emplace_back(std::move(std::make_unique<column>(groupby_value_column)));
  output_columns.emplace_back(std::move(std::make_unique<column>(groupby_order_column)));

  auto output_structs_column = make_structs_column(num_rows, std::move(output_columns), 0, rmm::device_buffer{}, stream, mr);

  auto output_table = std::make_unique<table>(table_view{{*output_structs_column}});

  printf("!!group_min_by flag 5\n");

  if (!output_table->get_column(0).has_nulls()) output_table->get_column(0).set_null_mask({}, 0);
  return std::make_unique<column>(std::move(output_table->get_column(0)));
}

}  // namespace detail
}  // namespace groupby
}  // namespace cudf
