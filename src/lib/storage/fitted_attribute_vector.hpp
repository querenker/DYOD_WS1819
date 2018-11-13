#pragma once

#include <vector>

#include "base_attribute_vector.hpp"

namespace opossum {

    template<typename T>
    class FittedAttributeVector : public BaseAttributeVector {
     public:
        FittedAttributeVector(const ChunkOffset num_elements) {
            _values.resize(num_elements);
        }
        ~FittedAttributeVector() = default;

        ValueID get(const size_t offset) const override {
            DebugAssert(offset < _values.size(), "invalid offset");
            return ValueID {_values[offset]};
        }

        void set(const size_t offset, const ValueID value_id) override {
            DebugAssert(offset < _values.size(), "invalid offset");
            DebugAssert(value_id <= ValueID{std::numeric_limits<T>::max()}, "invalid value_id");
            _values[offset] = value_id;
        }

        size_t size() const override {
            return _values.size();
        }

        AttributeVectorWidth width() const override {
            return sizeof(T);
        }

    protected:
        std::vector<T> _values;
    };
} //namespace opossum
