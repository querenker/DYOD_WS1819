#include "table_scan.hpp"
#include "../resolve_type.hpp"
#include "../storage/table.hpp"

namespace opossum {

template <typename t>
class TableScanImpl;

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {
  //const std::string& column_type = in->get_output()->column_type(column_id);
  //_table_scan_impl = make_unique_by_data_type<Noncopyable, BaseTableScanImpl>(column_type);
}

TableScan::~TableScan() = default;

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const std::string& column_type = _input_left->get_output()->column_type(_column_id);
  auto impl = make_shared_by_data_type<BaseTableScanImpl, TableScanImpl>(column_type);

  //return _table_scan_impl->_on_execute();
  return nullptr;
}

}  // namespace opossum