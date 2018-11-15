#include "storage_manager.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(!has_table(name), "this table name is already used: " + name);
  _tables[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "this table name does not exist: " + name);
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.find(name) != _tables.end(); }

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>(_tables.size());
  std::transform(_tables.cbegin(), _tables.cend(), table_names.begin(),
                 [](auto value_pair) { return value_pair.first; });
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& table_pair : _tables) {
    const auto table_name = table_pair.first;
    const auto column_count = table_pair.second->column_count();
    const auto row_count = table_pair.second->row_count();
    const auto chunk_count = table_pair.second->chunk_count();
    out << table_name << ' ' << column_count << ' ' << row_count << ' ' << chunk_count << std::endl;
  }
}

void StorageManager::reset() { StorageManager::get()._tables.clear(); }

}  // namespace opossum
