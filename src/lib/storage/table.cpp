#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size) { add_new_chunk(); }

void Table::add_segment_to_chunk(std::shared_ptr<Chunk> chunk, const std::string& type) {
  auto new_segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
  chunk->add_segment(new_segment);
}

void Table::add_new_chunk() {
  auto chunk = std::make_shared<Chunk>();
  for (const auto& column_type : _column_types) {
    add_segment_to_chunk(chunk, column_type);
  }
  _chunks.push_back(chunk);
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "cannot add columns if rows already exist");
  Assert(std::find(_column_names.begin(), _column_names.end(), name) == _column_names.end(),
         "column with name " + name + " already exist");
  _column_names.push_back(name);
  _column_types.push_back(type);
  add_segment_to_chunk(_chunks[0], type);
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(_chunks.back()->size() <= _chunk_size, "chunk contains more values than allowed");
  if (_chunks.back()->size() == _chunk_size) {
    add_new_chunk();
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const { return _column_names.size(); }

void Table::create_new_chunk() {
  // Implementation goes here
}

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for (uint64_t row_index = 0; row_index < _chunks.size(); row_index++) {
    row_count += _chunks[row_index]->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  DebugAssert(_chunks.size() > 0, "there must always be a chunk");
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto column_with_name = std::find(_column_names.begin(), _column_names.end(), column_name);
  Assert(column_with_name != _column_names.end(), "no column with this name " + column_name);
  return ColumnID(std::distance(_column_names.begin(), column_with_name));
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names[column_id]; }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types[column_id]; }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  return *(_chunks[chunk_id]);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  return *(_chunks[chunk_id]);
}

void Table::compress_chunk(ChunkID chunk_id) {
  std::cout << "compress_chunk start" << std::endl;
  DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  const auto chunk = _chunks[chunk_id];
  auto new_chunk = std::make_shared<Chunk>();
  std::cout << "compress_chunk middle" << std::endl;
  for (ColumnID column_id = ColumnID{0}; column_id < chunk->column_count(); column_id++) {
    auto dictionary_segment =
        make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type(column_id), chunk->get_segment(column_id));
    new_chunk->add_segment(dictionary_segment);
  }
  _chunks[chunk_id] = new_chunk;
  std::cout << "compress_chunk end" << std::endl;
}

void Table::emplace_chunk(Chunk&& chunk) {
  DebugAssert(chunk.size() <= _chunk_size, "chunk is too big");
  //TODO should we check data types as well?
  DebugAssert(chunk.column_count() == column_count(), "chunk column count does not match");

  //TODO multithreading?
  if (row_count() == 0) {
    _chunks[0] = std::make_shared<Chunk>(std::move(chunk));
    return;
  }

  //TODO do we need this assert?
  DebugAssert(_chunks.back()->size() == _chunk_size, "last chunk is not full");
  _chunks.emplace_back(std::make_shared<Chunk>(std::move(chunk)));
}

}  // namespace opossum
