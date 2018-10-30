#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _segments.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(), "wrong number of items in passed row");
  for (size_t column_index = 0; column_index < values.size(); column_index++) {
    _segments[column_index]->append(values[column_index]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _segments[column_id]; }

uint16_t Chunk::column_count() const { return _segments.size(); }

uint32_t Chunk::size() const {
  if (column_count() > 0) return _segments[0]->size();
  return 0;
}

}  // namespace opossum
