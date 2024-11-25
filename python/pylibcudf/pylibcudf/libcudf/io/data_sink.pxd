# Copyright (c) 2023-2024, NVIDIA CORPORATION.
from pylibcudf.exception_handler cimport libcudf_exception_handler


cdef extern from "cudf/io/data_sink.hpp" \
        namespace "cudf::io" nogil:

    cdef cppclass data_sink:
        pass
