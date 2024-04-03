#include <cudf/io/memory_resource.hpp>
#include <cudf/lists/lists_column_view.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/structs/structs_column_view.hpp>
#include <cudf/table/table_view.hpp>
#include <sstream>
#include <arpa/inet.h>

#include "jcudf_serializer.hpp"

uint64_t type_size(cudf::type_id type) {
  switch(type) {
    case cudf::type_id::EMPTY:
    case cudf::type_id::STRING:
    case cudf::type_id::LIST:
    case cudf::type_id::STRUCT:
      return 0;
    case cudf::type_id::INT8:
    case cudf::type_id::UINT8:
    case cudf::type_id::BOOL8:
      return 1;
    case cudf::type_id::INT16:
    case cudf::type_id::UINT16:
      return 2;
    case cudf::type_id::INT32:
    case cudf::type_id::UINT32:
    case cudf::type_id::FLOAT32:
    case cudf::type_id::TIMESTAMP_DAYS:
    case cudf::type_id::DURATION_DAYS:
    case cudf::type_id::DECIMAL32:
      return 4;
    case cudf::type_id::INT64:
    case cudf::type_id::UINT64:
    case cudf::type_id::FLOAT64:
    case cudf::type_id::TIMESTAMP_SECONDS:
    case cudf::type_id::TIMESTAMP_MILLISECONDS:
    case cudf::type_id::TIMESTAMP_MICROSECONDS:
    case cudf::type_id::TIMESTAMP_NANOSECONDS:
    case cudf::type_id::DURATION_SECONDS:
    case cudf::type_id::DURATION_MILLISECONDS:
    case cudf::type_id::DURATION_MICROSECONDS:
    case cudf::type_id::DURATION_NANOSECONDS:
    case cudf::type_id::DECIMAL64:
      return 8;
    case cudf::type_id::DECIMAL128:
      return 16;
    default:
      return 0;
  }
}

template <typename T>
std::size_t set(uint8_t* header, std::size_t offset, T el) {
  T& out = *(reinterpret_cast<T*>(header + offset));
  if constexpr(sizeof(T) > 4) { // need to apply htonl to upper/lower halves
    const uint32_t upper = htonl(static_cast<uint64_t>(el) >> 32);
    const uint32_t lower = htonl(static_cast<uint32_t>(el));
    out = static_cast<T>((static_cast<uint64_t>(lower) << 32 | upper));
  } else {
    out = static_cast<T>(htonl(static_cast<uint32_t>(el)));
  }
  return sizeof(T);
}

struct jcudf_column_header_serializer {
static std::size_t serialize_header(
  host_col_data const& col, 
  uint8_t* header,
  std::size_t offset) {
    std::size_t begin_offset = offset;
    offset += set(header, offset, static_cast<int32_t>(col.type_id));
    offset += set(header, offset, col.scale);
    offset += set(header, offset, col.null_count);


    //TODO:
     //if (col.num_children() > 0) {
     //  switch(col.type().id()) {
     //    case cudf::type_id::LIST:
     //      sink.write(static_cast<int>(col.child(0).size()));
     //      break;
     //    case cudf::type_id::STRUCT:
     //      sink.write(static_cast<int>(col.num_children()));
     //      break;
     //    default:
     //      std::stringstream ss;
     //      ss << "Unexpected nested type: " << static_cast<int32_t>(col.type().id()); 
     //      throw std::runtime_error(ss.str().c_str());

     //    auto child_iter = col.child_begin();
     //    while (child_iter != col.child_end()) {
     //      offset += serialize_header(*child_iter, header + offset);
     //    }
     //  }
     //}
    return offset - begin_offset;
  }

uint64_t get_serialized_size(cudf::column_view const& col) const {
  // 4 byte type ID, 4 byte scale, 4 byte null count
  uint64_t total = 4 + 4 + 4;

  if (col.num_children() > 0) {
    switch(col.type().id()) {
      case cudf::type_id::LIST:
        total += 4; // 4-byte child row count
        break;
      case cudf::type_id::STRUCT:
        total += 4; // 4-byte child count
        break;
      default:
        std::stringstream ss;
        ss << "Unexpected neted type: " << static_cast<int32_t>(col.type().id()); 
        throw std::runtime_error(ss.str().c_str());

      auto child_iter = col.child_begin();
      while (child_iter != col.child_end()) {
        total += get_serialized_size(*child_iter);
      }
    }
  }

  return total;
}
};

struct jcudf_table_header_serializer {
static const int32_t SER_FORMAT_MAGIC_NUMBER = 0x43554446;
static const short VERSION_NUMBER = 0x0000;

static std::size_t serialize_header(
  cudf::size_type num_columns,
  cudf::size_type num_rows,
  uint8_t* header) {
    std::size_t offset = 0;
    offset += set(header, offset, SER_FORMAT_MAGIC_NUMBER);
    offset += set(header, offset, VERSION_NUMBER);
    offset += set(header, offset, num_columns);
    offset += set(header, offset, num_rows);
    return offset; //14 bytes
}
};

bool is_nested(cudf::type_id type) {
  switch (type) {
    case cudf::type_id::LIST:
    case cudf::type_id::STRUCT:
      return true;
    default:
      return false;
  }
}

bool needs_validity_buffer(cudf::column_view const& col) {
  // TODO: check for unknown nulls?
  return col.null_count() > 0;
}

bool needs_validity_buffer(host_col_data const& col_data) {
  // TODO: check for unknown nulls?
  return col_data.null_count > 0;
}

bool needs_offset_buffer(cudf::type_id type) {
  switch (type) {
    case cudf::type_id::LIST:
    case cudf::type_id::STRING:
      return true;
    default:
      return false;
  }
}

uint64_t get_sliced_serialized_data_size(
  cudf::column_view const& col, 
  uint64_t row_offset, 
  uint64_t num_rows) {
  return 0;
}

uint64_t get_validity_length(uint64_t num_rows) {
  return (num_rows + 7) / 8;
}

uint64_t get_raw_string_data_size(
  cudf::column_view const& col, uint64_t row_offset, uint64_t num_rows) {
  if (num_rows <= 0) {
    return 0;
  }
  cudf::strings_column_view string_col(col);
  auto begin_it = string_col.offsets_begin() + row_offset;
  auto end_it = begin_it + num_rows;
  return end_it - begin_it;
}

jcudf_serializer::jcudf_serializer(cudf::table_view const& table) {
  // header size for this table
  header_size = 4 + 2 + 4 + 4 + 8; // table header
  
  for (int i = 0; i < table.num_columns(); ++i) {
    header_size += 12; // type, scale, null_count
    auto const& col = table.column(i);
    auto d_data = col.data<uint8_t>();
    auto row_count = col.size();
    // TODO: better way of doing this? padding?
    auto byte_size = row_count * type_size(col.type().id());
    auto h_data = reinterpret_cast<uint8_t*>(
      cudf::io::get_host_memory_resource().allocate(byte_size));
    cudaMemcpyAsync(h_data, d_data, byte_size, cudaMemcpyDefault, 0);
    cudaStreamSynchronize(0);
    // TODO: do we need data buffer size and row count
    host_col_datas.push_back(host_col_data{
      col.type().id(), 
      col.type().scale(),
      h_data, 
      row_count, 
      byte_size,
      col.null_count()
    });
  }

  // we should be able to destroy table after this
}

jcudf_serializer::~jcudf_serializer() {
  for (auto& col :host_col_datas) {
    cudf::io::get_host_memory_resource().deallocate(col.data, col.byte_size);
  }
}

uint64_t jcudf_serializer::get_serialized_data_len(
  host_col_data const& col, uint64_t row_offset, uint64_t num_rows) const {
  uint64_t total_data_size = 0;
  auto type = col.type_id;
  auto const col_type_size = type_size(type);

  // validity buffer size
  if (needs_validity_buffer(col)) {
    total_data_size += get_validity_length(num_rows);
  }

  // offset buffer size
  // TODO: if (needs_offset_buffer(type)) {
  //  // offset vector
  //  total_data_size += (num_rows + 1) * sizeof(cudf::size_type); // offset_type?
  //  if (num_rows > 0) {
  //    // raw strings
  //    total_data_size += get_raw_string_data_size(col, row_offset, num_rows);
  //  }
  //} else if (col_type_size > 0) {
    total_data_size += col_type_size * num_rows;
  //}

  // TODO if (num_rows > 0 && is_nested(type)) {
  //  switch(type) {
  //    case cudf::type_id::LIST:
  //    {
  //      auto list_col = cudf::lists_column_view(col);
  //      auto child = list_col.child();
  //      auto child_begin = list_col.offsets_begin() + row_offset;
  //      auto child_start_row = *child_begin;
  //      auto child_num_rows = *(child_begin + num_rows) - child_start_row;
  //      total_data_size += get_serialized_data_len(
  //        child, child_start_row, child_num_rows);
  //    }
  //    case cudf::type_id::STRUCT:
  //    {
  //      auto struct_col = cudf::structs_column_view(col);
  //      auto child_it = struct_col.child_begin();
  //      while (child_it != struct_col.child_end()) {
  //        total_data_size += 
  //          get_sliced_serialized_data_size(*child_it, row_offset, num_rows);
  //        child_it++;
  //      }
  //    }
  //    default:
  //      break;
  //  }
  //}
  return total_data_size;
}

uint64_t jcudf_serializer::get_serialized_data_len(uint64_t row_offset, uint64_t num_rows) const {
  uint64_t total_data_size = 0;
  for (auto const& col : host_col_datas) {
    total_data_size += 
      get_serialized_data_len(col, row_offset, num_rows);
  }
  return total_data_size;
}

// need to encode against a buffer and write out a byte at a time
//uint64_t jcudf_serializer::copy_partial_validity(
//  host_col_data const& col, 
//  uint64_t dest_bit_offset,
//  uint64_t validity_bit_offset, 
//  uint64_t rows_left_in_batch) {

//  uint64_t rows_processed = 0;
//  uint64_t dest_start_bytes = dest_bit_offset / 8;
//  uint64_t dest_start_bit_offset = dest_bit_offset % 8;
//  uint64_t src_start_bytes = 
//    reinterpret_cast<uint64_t>(col.data) + (validity_bit_offset / 8);
//  
//  uint64_t src_start_bit_offset = validity_bit_offset % 8;
//  uint64_t max_to_copy = 1234; // TODO: this should be an argument
//  uint64_t available_dest_bits = (max_to_copy * 8) - dest_bit_offset;
//  uint64_t bits_to_copy = std::min(rows_left_in_batch, available_dest_bits);
//  rows_processed = bits_to_copy;

//  uint64_t last_index = (bits_to_copy + dest_start_bit_offset + 7) / 8;
//  uint8_t all_bits_set = 0xFF;
//  uint64_t first_src_ask = 
//    static_cast<uint8_t>(all_bits_set << dest_start_bit_offset);

//  auto src_shift = dest_start_bit_offset - src_start_bit_offset;
//  if (src_shift > 0) {
//    // common case
//    uint8_t* current = col.data + src_start_bytes;
//    uint8_t result = *current << src_shift;
//  } else if (src_shift < 0) {
//    // ran out of space
//  } else {
//    _sink.write(col.data + dest_start_bytes, (bits_to_copy + 7) / 8);
//  }

//  return rows_processed;
//}

//void jcudf_serializer::copy_sliced_validity(
//  host_col_data const& col, uint64_t row_offset, uint64_t num_rows) {

//  auto data = col.data;
//  auto validity_len = get_validity_length(num_rows);
//  auto byte_offset = row_offset / 8;
//  auto bytes_left = validity_len;

//  auto lshift = row_offset % 8;
//  if (lshift == 0) {
//    _sink.write(data + byte_offset, bytes_left);
//  } else {
//    auto rows_left_in_batch = num_rows;
//    auto validity_bit_offset = row_offset;
//    auto rows_processed_so_far = 0;
//    while (rows_left_in_batch > 0) {
//      auto rows_processed =  0; // TODO
//        //copy_partial_validity(
//        //  col, rows_processed_so_far, validity_bit_offset, rows_left_in_batch);

//      rows_left_in_batch -= rows_processed;
//      validity_bit_offset += rows_processed;
//      // TODO: need to reset this if we stop processing and come back
//      rows_processed_so_far = rows_processed;
//    }
//  }

//  _sink.write(data, col.size);
//}

void jcudf_serializer::copy_sliced_and_pad(
  jcudf_serializer_sink& sink,
  host_col_data const& col, 
  uint64_t src_offset, 
  uint64_t bytes_to_copy) {
  sink.write(col.data + src_offset, bytes_to_copy);
}

void jcudf_serializer::copy_basic_data(
  jcudf_serializer_sink& sink,
  host_col_data const& col, 
  uint64_t row_offset, 
  uint64_t num_rows) {
  auto const type_id = col.type_id;
  auto const data_size = type_size(type_id);
  uint64_t bytes_to_copy = num_rows * data_size;
  uint64_t src_offset = row_offset * data_size;
  copy_sliced_and_pad(sink, col, src_offset, bytes_to_copy);
}

void jcudf_serializer::write_column(
  jcudf_serializer_sink& sink,
  host_col_data const& col, 
  uint64_t row_offset, 
  uint64_t num_rows) {

  // TODO: lets skip validity for now
  if (false && needs_validity_buffer(col)) {
    copy_sliced_validity(col, row_offset, num_rows);
  }

  // TODO: handle types with offsets here
  copy_basic_data(sink, col, row_offset, num_rows); 
} 

std::size_t jcudf_serializer::write_header(
    cudf::size_type num_columns,
    uint64_t row_offset, 
    uint64_t num_rows) {
  auto data_len = get_serialized_data_len(row_offset, num_rows);
  // 3x32bit, 1x16bit
  auto offset = jcudf_table_header_serializer::serialize_header(num_columns, num_rows, header.get());
  for (auto const& col : host_col_datas) {
    // 3x32 bit numbers of basic cols
    offset += jcudf_column_header_serializer::serialize_header(col, header.get(), offset);
  }
  // 8 byte
  offset += set(header.get(), offset, data_len);
  return offset;
}

std::size_t jcudf_serializer::write_data(
    jcudf_serializer_sink& sink, uint64_t row_offset, uint64_t num_rows) {
  // write header
  header.reset(new uint8_t[header_size]);
  write_header(host_col_datas.size(), row_offset, num_rows);

  auto sink_start = sink.bytes_written();
  sink.write(header.get(), header_size);

  // loop through columns
  for (auto const& col : host_col_datas) {
    // emplace back?
    write_column(sink, col, row_offset, num_rows);
  }

  auto bytes_written = sink.bytes_written() - sink_start;
  return bytes_written;
}