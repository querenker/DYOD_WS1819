#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {

 protected:
    class BaseTableScanImpl {
    public:
        virtual ~BaseTableScanImpl() = default;


        virtual std::shared_ptr<const Table> on_execute(const TableScan& tableScan) = 0;


    };
  std::shared_ptr<const Table> _on_execute() override;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  std::unique_ptr<BaseTableScanImpl> _table_scan_impl;



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
      void _search_within_dictionary_segment(const std::shared_ptr<const std::vector<T>>& dictionary, const std::shared_ptr<const FittedAttributeVector<U>>& attribute_vector, const AllTypeVariant& search_value, const std::function<bool (T,T)>& comparator, const ChunkID chunk_id, std::shared_ptr<PosList>& pos_list) {
          const auto& values = attribute_vector->values();
          for (ChunkOffset chunk_offset{0}; chunk_offset < values.size();chunk_offset++) {
              if (comparator(dictionary->operator[](values[chunk_offset]), type_cast<T>(search_value))) {
                  pos_list->push_back(RowID{chunk_id, chunk_offset});
              }
          }
      }

      std::shared_ptr<const Table> on_execute(const TableScan& table_scan) override {
        const auto column_id = table_scan.column_id();
        const auto input_table = table_scan.input_left()->get_output();
        const auto scan_type = table_scan.scan_type();
        const auto search_value = table_scan.search_value();
        auto pos_list = std::make_shared<PosList>();

        bool reference_segment_processed = false;

        for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
            const Chunk& chunk = input_table->get_chunk(chunk_id);
            const std::shared_ptr<BaseSegment> segment = chunk.get_segment(column_id);
            const auto comparator = get_comparator(scan_type);

            if (std::dynamic_pointer_cast<ValueSegment<T>>(segment) != nullptr) {
                const auto value_segment = std::static_pointer_cast<ValueSegment<T>>(segment);
                const auto& values = value_segment->values();
                for (ChunkOffset chunk_offset{0};chunk_offset < values.size();chunk_offset++) {
                    if (comparator(values[chunk_offset], type_cast<T>(search_value))) {
                        pos_list->push_back(RowID{chunk_id, chunk_offset});
                    }
                }
            } else if (std::dynamic_pointer_cast<DictionarySegment<T>>(segment) != nullptr){
                const auto dictionary_segment = std::static_pointer_cast<DictionarySegment<T>>(segment);
                const auto attribute_vector = dictionary_segment->attribute_vector();
                const auto dictionary = dictionary_segment->dictionary();
                
                switch (attribute_vector->width()) {
                    case sizeof(uint8_t): {
                        const auto fitted_attribute_vector = std::static_pointer_cast<const FittedAttributeVector<uint8_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint8_t>(dictionary, fitted_attribute_vector, search_value, comparator, chunk_id, pos_list);
                        break;
                    }
                    case sizeof(uint16_t): {
                        const auto fitted_attribute_vector = std::static_pointer_cast<const FittedAttributeVector<uint16_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint16_t>(dictionary, fitted_attribute_vector, search_value, comparator, chunk_id, pos_list);
                        break;
                    }
                    case sizeof(uint32_t): {
                        const auto fitted_attribute_vector = std::static_pointer_cast<const FittedAttributeVector<uint32_t>>(attribute_vector);
                        DebugAssert(fitted_attribute_vector != nullptr, "cast failed");
                        _search_within_dictionary_segment<uint32_t>(dictionary, fitted_attribute_vector, search_value, comparator, chunk_id, pos_list);
                        break;
                    }

                    default: {
                        Fail("unknown attribute vector width: " + attribute_vector->width());
                    }
                }
            } else if (std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr) {
                DebugAssert(chunk_id == 0, "there should always be only 1 chunk for reference segments");
                reference_segment_processed = true;
                const auto reference_segment = std::static_pointer_cast<ReferenceSegment>(segment);
                const auto old_pos_list = reference_segment->pos_list();

                for (ChunkOffset chunk_offset{0}; chunk_offset < reference_segment->size();chunk_offset++) {
                    if (comparator(type_cast<T>(reference_segment->operator[](chunk_offset)), type_cast<T>(search_value))) {
                        pos_list->push_back(old_pos_list->operator[](chunk_offset));
                    }
                }

            } else {
                Fail("unknown segment type");
            }
        }

        auto result_table = std::make_shared<Table>();
        Chunk chunk;
        for (ColumnID col_id{0};col_id < input_table->column_count();col_id++) {
            result_table->add_column(input_table->column_name(col_id), input_table->column_type(col_id));

            if (reference_segment_processed) {
                //a reference segment always only references one table, thus we can use the second input_left()
                const auto segment = std::make_shared<ReferenceSegment>(table_scan.input_left()->input_left()->get_output(), col_id, pos_list);
                chunk.add_segment(segment);
            } else {
                const auto segment = std::make_shared<ReferenceSegment>(input_table, col_id, pos_list);
                chunk.add_segment(segment);
            }


        }

        result_table->emplace_chunk(std::move(chunk));
        return result_table;
    }


  };
  public:
    TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
              const AllTypeVariant search_value);

    ~TableScan();

    ColumnID column_id() const;
    ScanType scan_type() const;
    const AllTypeVariant& search_value() const;
};

}  // namespace opossum
