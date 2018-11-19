#include "table_scan.hpp"
#include "../resolve_type.hpp"
#include "../storage/table.hpp"

namespace opossum {

    TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
              const AllTypeVariant search_value) : AbstractOperator(in), _column_id {column_id}, _scan_type {scan_type}, _search_value {search_value} {
        //const std::string& column_type = in->get_output()->column_type(column_id);
        //_table_scan_impl = make_unique_by_data_type<Noncopyable, BaseTableScanImpl>(column_type);
    }

    ColumnID TableScan::column_id() const {
        return _column_id;
    }

    ScanType TableScan::scan_type() const {
        return _scan_type;
    }

    const AllTypeVariant& TableScan::search_value() const {
        return _search_value;
    }

    std::shared_ptr<const Table> TableScan::_on_execute() {
        const std::string& column_type = _input_left->get_output()->column_type(_column_id);
        auto impl = make_shared_by_data_type<BaseTableScanImpl>(column_type);

        //return _table_scan_impl->_on_execute();
        return nullptr;
    }


    template <typename T>
    class BaseTableScanImpl {

        std::shared_ptr<const Table> on_execute(const std::shared_ptr<const Table> in, ColumnID columnID, const ScanType scanType, const AllTypeVariant search_value) {

            for (ChunkID chunk_id = ChunkID{0};chunk_id < in->chunk_count();chunk_id++) {
                const Chunk& chunk = in->get_chunk(chunk_id);
                const std::shared_ptr<BaseSegment> segment = chunk.get_segment(columnID);
                const auto comparator = get_comparator(scanType);

                //TODO: remove inefficient access
                for (ChunkOffset offset{0};offset < chunk.size();offset++) {
                    //if (comparator(segment[offset], search_value)) {

                        //TODO fancy stuff with reference segment
                        std::cout << "we need a reference segment here" << std::endl;
                    //}
                }
            }

            return nullptr;
        }

        auto get_comparator(ScanType scanType) {
            switch (scanType) {
                case ScanType::OpEquals: {
                    return [](auto left, auto right) { return left == right; };
                }

                case ScanType::OpNotEquals: {
                    return [](auto left, auto right) { return left != right; };
                }

                case ScanType::OpLessThan: {
                    return [](auto left, auto right) { return left < right; };
                }

                case ScanType::OpLessThanEquals: {
                    return [](auto left, auto right) { return left <= right; };
                }

                case ScanType::OpGreaterThan: {
                    return [](auto left, auto right) { return left > right; };
                }

                case ScanType::OpGreaterThanEquals: {
                    return [](auto left, auto right) { return left >= right; };
                }
            }
        }

    };


}