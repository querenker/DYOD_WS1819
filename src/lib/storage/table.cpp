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

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size) {
  add_new_chunk();
}

void Table::add_new_chunk() {
  auto chunk = std::make_shared<Chunk>();
  for(auto &column : _columns) {
    auto new_segment = make_shared_by_data_type<BaseSegment, ValueSegment>(column.second); 
    chunk->add_segment(new_segment);
  }
  _chunks.push_back(chunk);
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "cannot add columns if rows already exist");
  _columns.push_back(std::make_pair(name, type));
  auto new_segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
  _chunks[0]->add_segment(new_segment);
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(_chunks.back()->size() <= _chunk_size, "chunk contains more values than allowed");
  if(_chunks.back()->size() == _chunk_size) {
    add_new_chunk();
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const {
  return _columns.size();
}

uint64_t Table::row_count() const {
  return (_chunks.size() - 1) * _chunk_size + _chunks.back()->size();
}

ChunkID Table::chunk_count() const {
  DebugAssert(_chunks.size() > 0, "there must always be a chunk");
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Implementation goes here
  auto column_iter = find_if(_columns.begin(), _columns.end(), [&column_name](auto &column){
    return column.first.compare(column_name) == 0;
  });
  if(column_iter == _columns.end()) {
    throw std::runtime_error("no column with this name " + column_name);
  }
  return ColumnID(std::distance(_columns.begin(), column_iter));
}

uint32_t Table::chunk_size() const {
  return _chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  auto column_names = std::vector<std::string>(_columns.size());
  std::transform(_columns.begin(), _columns.end(), column_names.begin(), [](auto &column){
    return column.first;
  });
  return std::move(column_names);
}

const std::string& Table::column_name(ColumnID column_id) const {
  return _columns[column_id].first;
}

const std::string& Table::column_type(ColumnID column_id) const {
  return _columns[column_id].second;
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return *(_chunks[chunk_id]);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return *(_chunks[chunk_id]);
}
}  // namespace opossum
