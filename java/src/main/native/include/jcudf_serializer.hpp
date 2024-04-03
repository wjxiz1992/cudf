#pragma once

#include <vector>
#include <cudf/table/table_view.hpp>

struct host_col_data;
class jcudf_serializer;

class jcudf_serializer_sink {
  uint8_t* _buffer;
  uint64_t _write_offset;
  jcudf_serializer* _serializer;

public:
  jcudf_serializer_sink(uint8_t* buffer): 
    _buffer(buffer), _write_offset(0) {}

  void initialize(jcudf_serializer* serializer) {
    _serializer = serializer;
  }

  bool has_next() const;
  uint64_t next() const;

  template<typename T>
  void write(T data){
    *reinterpret_cast<T*>(_buffer + _write_offset) = data;
    _write_offset += sizeof(T);
  }

  template<typename T>
  void write(T* data, uint64_t sz){
    memcpy(_buffer + _write_offset, data, sz);
    _write_offset += sz;
  }

  std::size_t bytes_written() const {
    return _write_offset;
  }
};


struct host_col_data {
  cudf::type_id type_id;
  int32_t scale;
  uint8_t* data;
  cudf::size_type size;
  uint64_t byte_size;
  cudf::size_type null_count;
};

class jcudf_serializer {
  std::unique_ptr<uint8_t> header;
  std::size_t header_size;
  std::vector<host_col_data> host_col_datas;

  // TODO: do we need this
  uint64_t get_serialized_data_len(
    host_col_data const& col, uint64_t row_offset, uint64_t num_rows) const;
  uint64_t get_serialized_data_len(
    uint64_t row_offset, uint64_t num_rows) const;
  uint64_t copy_partial_validity(
    host_col_data const& col, 
    uint64_t dest_bit_offset,
    uint64_t validity_bit_offset, 
    uint64_t rows_left_in_batch);
  void copy_sliced_validity(
    host_col_data const& col, uint64_t row_offset, uint64_t num_rows);
  void copy_sliced_and_pad(
    jcudf_serializer_sink& sink,
    host_col_data const& col, 
    uint64_t src_offset, 
    uint64_t bytes_to_copy);
  void copy_basic_data(
    jcudf_serializer_sink& sink,
    host_col_data const& col, 
    uint64_t row_offset, 
    uint64_t num_rows);
  void write_column(
    jcudf_serializer_sink& sink,
    host_col_data const& col, 
    uint64_t row_offset, 
    uint64_t num_rows);
  std::size_t write_header(
    cudf::size_type num_columns, 
    uint64_t row_offset, 
    uint64_t num_rows);
public:
  jcudf_serializer(cudf::table_view const& table);

  ~jcudf_serializer();

  std::size_t write_data(jcudf_serializer_sink& sink, uint64_t row_offset, uint64_t num_rows);
};