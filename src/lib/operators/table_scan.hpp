#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
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
    virtual ~BaseTableScanImpl() = 0;

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


      std::shared_ptr<const Table> on_execute(const std::shared_ptr<const Table> in, ColumnID columnID,
                                            const ScanType scanType, const AllTypeVariant search_value) override {
        for (ChunkID chunk_id{0}; chunk_id < in->chunk_count(); chunk_id++) {
            const Chunk& chunk = in->get_chunk(chunk_id);
            const std::shared_ptr<BaseSegment> segment = chunk.get_segment(columnID);
            const auto comparator = get_comparator(scanType);

            const auto tmp = std::dynamic_pointer_cast<ValueSegment<T>>(segment);

            if (tmp != nullptr) {
                const auto values = tmp->values();
                for (const auto& value : values) {
                    if (comparator(value, type_cast<T>(search_value))) {
                        //TODO fancy stuff with reference segment
                        std::cout << "we need a reference segment here" << std::endl;
                    }
                }
            } else {
                //dictionarysegment or reference
            }
        }
        return nullptr;
    }


  };

  //  template <typename T>
  //  std::unique_ptr<BaseTableScanImpl> _table_scan_impl;
};

}  // namespace opossum
