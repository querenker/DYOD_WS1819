#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"
#include "../../lib/type_cast.hpp"
#include "../../lib/types.hpp"

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");

  // Test attributes
  auto attribute_vector = dict_col->attribute_vector();
  EXPECT_EQ(attribute_vector->get(0u), 1u);
  EXPECT_EQ(attribute_vector->get(1u), 3u);
  EXPECT_EQ(attribute_vector->get(2u), 0u);
  EXPECT_EQ(attribute_vector->get(3u), 3u);
  EXPECT_EQ(attribute_vector->get(4u), 2u);
  EXPECT_EQ(attribute_vector->get(5u), 1u);
}

TEST_F(StorageDictionarySegmentTest, ValueByValueId) {
  vc_str->append("name2");
  vc_str->append("name1");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  EXPECT_EQ(dict_col->value_by_value_id(opossum::ValueID{0}), "name1");
  EXPECT_EQ(dict_col->value_by_value_id(opossum::ValueID{1}), "name2");
  EXPECT_THROW(dict_col->value_by_value_id(opossum::ValueID{2}), std::exception);
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, Accessing) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  EXPECT_EQ(dict_col->get(0u), "Bill");
  EXPECT_EQ(dict_col->get(1u), "Steve");
  EXPECT_EQ(dict_col->get(2u), "Alexander");
  EXPECT_EQ(dict_col->get(3u), "Steve");
  EXPECT_EQ(dict_col->get(4u), "Hasso");
  EXPECT_EQ(dict_col->get(5u), "Bill");

  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[0u]), "Bill");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[1u]), "Steve");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[2u]), "Alexander");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[3u]), "Steve");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[4u]), "Hasso");
  EXPECT_EQ(opossum::type_cast<std::string>((*dict_col)[5u]), "Bill");

  if (IS_DEBUG) {
    EXPECT_THROW(dict_col->get(6u), std::exception);
    EXPECT_THROW((*dict_col)[6u], std::exception);
  }
}

TEST_F(StorageDictionarySegmentTest, Immutability) {
  vc_str->append("Eva");
  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  EXPECT_THROW(col->append("Not allowed!!!!"), std::exception);
}

TEST_F(StorageDictionarySegmentTest, AttributeWidthWideEnough) {
  vc_int->append(0);
  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);
  auto attribute_vector = dict_col->attribute_vector();
  EXPECT_EQ(attribute_vector->width(), 1u);

  vc_int = std::make_shared<opossum::ValueSegment<int>>();
  for (int16_t value = 0; value <= std::numeric_limits<uint8_t>::max(); value++) {
    vc_int->append(value);
  }
  col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);
  attribute_vector = dict_col->attribute_vector();
  EXPECT_EQ(attribute_vector->width(), 2u);

  vc_int = std::make_shared<opossum::ValueSegment<int>>();
  for (int32_t value = 0; value <= std::numeric_limits<uint16_t>::max(); value++) {
    vc_int->append(value);
  }
  col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);
  attribute_vector = dict_col->attribute_vector();
  EXPECT_EQ(attribute_vector->width(), 4u);
}
