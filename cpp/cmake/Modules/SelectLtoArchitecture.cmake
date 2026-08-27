# =============================================================================
# cmake-format: off
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# cmake-format: on
# =============================================================================

include_guard(GLOBAL)

# Select the common LTO base architecture and return it through output_variable.
function(cudf_select_lto_architecture output_variable)
  set(selected_architecture "${CUDF_LTO_ARCHITECTURE}")

  if(selected_architecture STREQUAL "")
    # CMAKE_CUDA_ARCHITECTURES is resolved against the active compiler by rapids-cmake. Prefer it
    # over CMAKE_CUDA_ARCHITECTURES_ALL, whose value depends on the CMake version and can therefore
    # contain architectures that the active compiler no longer supports.
    foreach(architecture IN LISTS CMAKE_CUDA_ARCHITECTURES)
      string(REGEX MATCH "^[0-9]+" numeric_architecture "${architecture}")
      if(numeric_architecture AND (NOT selected_architecture OR numeric_architecture LESS
                                                                selected_architecture)
      )
        set(selected_architecture "${numeric_architecture}")
      endif()
    endforeach()
  endif()

  if(selected_architecture STREQUAL "")
    # Preserve support for symbolic CMake values such as `all` and `all-major`.
    foreach(architecture IN LISTS CMAKE_CUDA_ARCHITECTURES_ALL)
      string(REGEX MATCH "^[0-9]+" numeric_architecture "${architecture}")
      if(numeric_architecture AND (NOT selected_architecture OR numeric_architecture LESS
                                                                selected_architecture)
      )
        set(selected_architecture "${numeric_architecture}")
      endif()
    endforeach()
  endif()

  if(NOT selected_architecture MATCHES "^[0-9]+$")
    message(FATAL_ERROR "CUDF_LTO_ARCHITECTURE must be a numeric architecture")
  endif()

  set(${output_variable}
      "${selected_architecture}"
      PARENT_SCOPE
  )
endfunction()
