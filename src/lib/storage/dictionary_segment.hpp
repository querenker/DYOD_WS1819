#pragma once

#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_segment.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "value_segment.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment)
      : _dictionary(std::make_shared<std::vector<T>>()) {
    DebugAssert(base_segment->size() <= std::numeric_limits<ChunkOffset>::max(), "too many values in a segment");
    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(base_segment);
    DebugAssert(value_segment != nullptr, "expected to get a value segment");

    // initialize dictionary
    auto unique_values = std::set<T>();
    for (const auto& value : value_segment->values()) {
      unique_values.insert(value);
    }

    const auto num_unique_elements = unique_values.size();
    _dictionary->reserve(num_unique_elements);
    for (const auto& unique_value : unique_values) {
      _dictionary->push_back(unique_value);
    }

    // initialize attribute vector
    if (num_unique_elements <= std::numeric_limits<uint8_t>::max()) {
      _initialize_attribute_vector<uint8_t>(value_segment, unique_values);
    } else if (num_unique_elements <= std::numeric_limits<uint16_t>::max()) {
      _initialize_attribute_vector<uint16_t>(value_segment, unique_values);
    } else if (num_unique_elements <= std::numeric_limits<uint32_t>::max()) {
      _initialize_attribute_vector<uint32_t>(value_segment, unique_values);
    }
  }

  template <typename S>
  void _initialize_attribute_vector(const std::shared_ptr<ValueSegment<T>>& value_segment, const std::set<T>& unique_values) {
    std::vector<S> attributes;
    attributes.reserve(value_segment->size());

    for (const auto& value : value_segment->values()) {
      const auto set_pos = static_cast<ValueID>(std::distance(unique_values.cbegin(), unique_values.find(value)));
      DebugAssert(set_pos < unique_values.size(),
                  "The value " + type_cast<std::string>(value) + " is not in the dictionary");
      attributes.push_back(set_pos);
    }
    _attribute_vector = std::make_shared<FittedAttributeVector<S>>(std::move(attributes));
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t index) const override { return get(index); }

  // return the value at a certain position.
  const T get(const size_t index) const {
    DebugAssert(index < _attribute_vector->size(), "invalid index");
    return (*_dictionary)[(*_attribute_vector).get(index)];
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override { Fail("dictionary segments are immutable"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const {
    DebugAssert(value_id != INVALID_VALUE_ID && value_id < _dictionary->size(), "invalid value id");
    return (*_dictionary)[value_id];
  }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto lower_bound_value_pos = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (lower_bound_value_pos == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->cbegin(), lower_bound_value_pos));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto upper_bound_value_pos = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (upper_bound_value_pos == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->cbegin(), upper_bound_value_pos));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
