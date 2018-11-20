#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  class BaseTableScanImpl {
  public:
    virtual ~BaseTableScanImpl() = default;


    virtual std::shared_ptr<const Table> on_execute(const std::shared_ptr<const Table> in, ColumnID columnID,
                                                    const ScanType scanType, const AllTypeVariant search_value) = 0;
  };

  template <typename T>
  class TableScanImpl : public BaseTableScanImpl {
  public:
      ~TableScanImpl() = default;

  protected:
      std::function<bool (T,T)> get_comparator(ScanType scanType) {
          std::function<bool (T,T)> result;
          switch (scanType) {
              case ScanType::OpEquals: {
                  result = [](T left, T right) { return left == right; };
                  break;
              }

              case ScanType::OpNotEquals: {
                  result =[](T left, T right) { return left != right; };
                  break;
              }

              case ScanType::OpLessThan: {
                  result = [](T left, T right) { return left < right; };
                  break;
              }

              case ScanType::OpLessThanEquals: {
                  result = [](T left, T right) { return left <= right; };
                  break;
              }

              case ScanType::OpGreaterThan: {
                  result = [](T left, T right) { return left > right; };
                  break;
              }

              case ScanType::OpGreaterThanEquals: {
                  result = [](T left, T right) { return left >= right; };
                  break;
              }
          }
          return result;
      }

      template <typename U>
      void _search_within_dictionary_segment(const std::shared_ptr<const std::vector<T>>& dictionary, const std::shared_ptr<const FittedAttributeVector<U>>& attribute_vector, const AllTypeVariant& search_value, const std::function<bool (T,T)>& comparator) {
          for (const U value : attribute_vector->values()) {
              if (comparator(dictionary->operator[](value), type_cast<T>(search_value))) {
                  //TODO fancy stuff with reference segment
                  std::cout << "we need a reference segment here" << std::endl;
              }
          }
      }

      std::shared_ptr<const Table> on_execute(const std::shared_ptr<const Table> in, ColumnID columnID,
                                            const ScanType scanType, const AllTypeVariant search_value) override {
        for (ChunkID chunk_id{0}; chunk_id < in->chunk_count(); chunk_id++) {
            const Chunk& chunk = in->get_chunk(chunk_id);
            const std::shared_ptr<BaseSegment> segment = chunk.get_segment(columnID);
            const auto comparator = get_comparator(scanType);

            if (std::dynamic_pointer_cast<ValueSegment<T>>(segment) != nullptr) {
                const auto value_segment = std::static_pointer_cast<ValueSegment<T>>(segment);
                const auto values = value_segment->values();
                for (const auto& value : values) {
                    if (comparator(value, type_cast<T>(search_value))) {
                        //TODO fancy stuff with reference segment
                        std::cout << "we need a reference segment here" << std::endl;
                    }
                }
            } else if (std::dynamic_pointer_cast<DictionarySegment<T>>(segment) != nullptr ){
                const auto dictionary_segment = std::static_pointer_cast<DictionarySegment<T>>(segment);
                const auto attribute_vector = dictionary_segment->attribute_vector();
                const auto dictionary = dictionary_segment->dictionary();
                
                switch (attribute_vector->width()) {
                    case sizeof(uint8_t): {
                        const auto fitted_attribute_vector = std::dynamic_pointer_cast<const FittedAttributeVector<uint8_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint8_t>(dictionary, fitted_attribute_vector, search_value, comparator);
                        break;
                    }
                    case sizeof(uint16_t): {
                        const auto fitted_attribute_vector = std::dynamic_pointer_cast<const FittedAttributeVector<uint16_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint16_t>(dictionary, fitted_attribute_vector, search_value, comparator);
                        break;
                    }
                    case sizeof(uint32_t): {
                        const auto fitted_attribute_vector = std::dynamic_pointer_cast<const FittedAttributeVector<uint32_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint32_t>(dictionary, fitted_attribute_vector, search_value, comparator);
                        break;
                    }
                    //TODO make sure one case is handled?
                }
            } else {
                //TODO reference segment
            }
        }
        return nullptr;
    }


  };

  //  template <typename T>
  //  std::unique_ptr<BaseTableScanImpl> _table_scan_impl;
};

}  // namespace opossum
