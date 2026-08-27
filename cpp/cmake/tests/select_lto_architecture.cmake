# =============================================================================
# cmake-format: off
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# cmake-format: on
# =============================================================================

include("${CUDF_REPOSITORY_DIR}/cpp/cmake/Modules/SelectLtoArchitecture.cmake")

# Verify that the selected LTO architecture matches the expected value.
function(assert_lto_architecture expected configured_architectures all_architectures override)
  set(CMAKE_CUDA_ARCHITECTURES "${configured_architectures}")
  set(CMAKE_CUDA_ARCHITECTURES_ALL "${all_architectures}")
  set(CUDF_LTO_ARCHITECTURE "${override}")

  cudf_select_lto_architecture(actual)

  if(NOT actual STREQUAL expected)
    message(FATAL_ERROR "Expected LTO architecture ${expected}, got ${actual}")
  endif()
endfunction()

# CMake 4.0's static list still contains SM50 when paired with CUDA 13, while rapids-cmake's
# compiler-aware configured list correctly begins at SM75.
assert_lto_architecture(
  75 "75-real;80-real;86-real;90a-real;100f-real;120a-real;120" "50;52;60;61;70;75;80;86;90" ""
)

# CUDA 12 continues to use SM70 as its common LTO base.
assert_lto_architecture(
  70 "70-real;75-real;80-real;86-real;90a-real;90-virtual" "50;52;60;61;70;75;80;86;90" ""
)

# An explicit user override remains authoritative.
assert_lto_architecture(80 "75-real;80-real" "50;52;60;61;70;75;80" 80)

# Preserve the existing fallback for symbolic CMake architecture values.
assert_lto_architecture(70 all "70;75;80;86;90" "")
