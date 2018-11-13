#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
    t.add_column("col_3", "int");
    t.add_column("col_4", "int");
    t.add_column("col_5", "int");
  }

  Table t{2};
};

TEST_F(StorageTableTest, AddColumnNameTwice) { EXPECT_THROW(t.add_column("col_1", "int"), std::exception); }

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,", 1, 2, 3});
  t.append({6, "world", 1, 2, 3});
  t.append({3, "!", 1, 2, 3});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});

  if (IS_DEBUG) {
    EXPECT_THROW(t.get_chunk(ChunkID{1}), std::exception);
  }

  t.append({4, "Hello,", 1, 2, 3});
  t.append({6, "world", 1, 2, 3});
  t.append({3, "!", 1, 2, 3});
  t.get_chunk(ChunkID{1});

  const Chunk& chunk = t.get_chunk(ChunkID{0});
  const auto segment = chunk.get_segment(ColumnID{0});
  EXPECT_EQ(type_cast<int>((*segment)[0]), 4);
  EXPECT_EQ(type_cast<int>((*segment)[1]), 6);
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 5u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,", 1, 2, 3});
  t.append({6, "world", 1, 2, 3});
  t.append({3, "!", 1, 2, 3});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnNames) {
  std::vector<std::string> expected_names(5);
  expected_names[0] = "col_1";
  expected_names[1] = "col_2";
  expected_names[2] = "col_3";
  expected_names[3] = "col_4";
  expected_names[4] = "col_5";
  EXPECT_EQ(t.column_names(), expected_names);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_EQ(t.column_id_by_name("col_5"), 4u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) {
  t.append({4, "Hello,", 1, 2, 3});
  t.append({6, "world", 1, 2, 3});
  t.append({3, "!", 1, 2, 3});
  t.compress_chunk(ChunkID{0});
  const auto& chunk = t.get_chunk(ChunkID{0});
  const auto segment = chunk.get_segment(ColumnID{0});
  EXPECT_EQ(type_cast<int>((*segment)[0]), 4);
  EXPECT_EQ(type_cast<int>((*segment)[1]), 6);
}

}  // namespace opossum
