#include <limits>
#include <memory>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/fitted_attribute_vector.hpp"

namespace opossum {

class StorageFittedAttributeVectorTest : public BaseTest {
 protected:
  std::shared_ptr<BaseAttributeVector> vector =
      std::make_shared<FittedAttributeVector<uint8_t>>(std::vector<uint8_t>{3, 4});
};

TEST_F(StorageFittedAttributeVectorTest, Size) { EXPECT_EQ(vector->size(), 2u); }

TEST_F(StorageFittedAttributeVectorTest, Get) {
  EXPECT_EQ(vector->get(ColumnID{0}), 3u);
  EXPECT_EQ(vector->get(ColumnID{1}), 4u);
}

TEST_F(StorageFittedAttributeVectorTest, Set) {
  vector->set(ColumnID{0}, ValueID{42u});
  vector->set(ColumnID{1}, ValueID{7u});
  EXPECT_EQ(vector->get(ColumnID{0}), 42u);
  EXPECT_EQ(vector->get(ColumnID{1}), 7u);
}

TEST_F(StorageFittedAttributeVectorTest, GetSetInvalidValues) {
  if (IS_DEBUG) {
    EXPECT_THROW(vector->set(ColumnID{0}, ValueID{std::numeric_limits<uint8_t>::max() + 1}), std::exception);

    EXPECT_THROW(vector->set(ColumnID{2}, ValueID{0}), std::exception);
    EXPECT_THROW(vector->get(ColumnID{2}), std::exception);
  }
}

TEST_F(StorageFittedAttributeVectorTest, Width) {
  EXPECT_EQ(vector->width(), 1u);

  vector = std::make_shared<FittedAttributeVector<uint16_t>>(std::vector<uint16_t>{42, 43});
  EXPECT_EQ(vector->width(), 2u);

  vector = std::make_shared<FittedAttributeVector<uint32_t>>(std::vector<uint32_t>{44, 45});
  EXPECT_EQ(vector->width(), 4u);
}

}  // namespace opossum
