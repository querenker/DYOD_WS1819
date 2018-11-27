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

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size) {
  DebugAssert(chunk_size > 0, "chunk size must be > 0");
  _chunks.push_back(std::make_shared<Chunk>());
}

void Table::_add_segment_to_chunk(std::shared_ptr<Chunk> chunk, const std::string& type) {
  auto new_segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
  chunk->add_segment(new_segment);
}

void Table::_init_chunk(std::shared_ptr<Chunk>& chunk) {
  for (const auto& column_type : _column_types) {
    _add_segment_to_chunk(chunk, column_type);
  }
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "cannot add columns if rows already exist");
  Assert(std::find(_column_names.cbegin(), _column_names.cend(), name) == _column_names.cend(),
         "column with name " + name + " already exist");
  _column_names.push_back(name);
  _column_types.push_back(type);
  _add_segment_to_chunk(_chunks[0], type);
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(_chunks.back()->size() <= _chunk_size, "chunk contains more values than allowed ");
  std::lock_guard lock(_chunk_mutex);
  if (_chunks.back()->size() == _chunk_size) {
    std::shared_ptr<Chunk> new_chunk = std::make_shared<Chunk>();
    _init_chunk(new_chunk);
    new_chunk->append(values);
    _emplace_chunk_without_locking(std::move(*new_chunk));
  } else {
    _chunks.back()->append(values);
  }
}

uint16_t Table::column_count() const { return _column_names.size(); }

void Table::create_new_chunk() {
  // Implementation goes here
}

uint64_t Table::row_count() const {
  uint64_t row_count = 0u;
  for (const auto& chunk : _chunks) {
    row_count += chunk->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  DebugAssert(_chunks.size() > 0, "there must always be a chunk");
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto column_with_name = std::find(_column_names.cbegin(), _column_names.cend(), column_name);
  Assert(column_with_name != _column_names.cend(), "no column with this name " + column_name);
  return ColumnID(std::distance(_column_names.cbegin(), column_with_name));
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

template <class T>
Chunk& Table::_get_chunk_impl(T& self, ChunkID chunk_id) {
  DebugAssert(chunk_id < self._chunks.size(), "invalid chunk id");
  std::shared_lock lock(self._chunk_mutex);
  return *(self._chunks[chunk_id]);
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return _get_chunk_impl(*this, chunk_id);
  // DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  // return *(_chunks[chunk_id]);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return _get_chunk_impl(*this, chunk_id);
  // DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  // return *(_chunks[chunk_id]);
}

void Table::compress_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "invalid chunk id");
  const auto chunk = _chunks[chunk_id];
  auto new_chunk = std::make_shared<Chunk>();
  for (ColumnID column_id = ColumnID{0}; column_id < chunk->column_count(); column_id++) {
    auto dictionary_segment =
        make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type(column_id), chunk->get_segment(column_id));
    new_chunk->add_segment(dictionary_segment);
  }
  std::lock_guard lock(_chunk_mutex);
  _chunks[chunk_id] = std::move(new_chunk);
}

void Table::_emplace_chunk_without_locking(Chunk&& chunk) {
  DebugAssert(chunk.size() <= _chunk_size, "chunk is too big");
  // TODO(anyone) should we check data types as well?
  DebugAssert(chunk.column_count() == column_count(), "chunk column count does not match");

  if (row_count() == 0) {
    _chunks[0] = std::make_shared<Chunk>(std::move(chunk));
    return;
  }

  // TODO(anyone) do we need this assert?
  DebugAssert(_chunks.back()->size() == _chunk_size, "last chunk is not full");
  _chunks.emplace_back(std::make_shared<Chunk>(std::move(chunk)));
}

void Table::emplace_chunk(Chunk&& chunk) {
  std::lock_guard lock(_chunk_mutex);
  _emplace_chunk_without_locking(std::move(chunk));
}

}  // namespace opossum
