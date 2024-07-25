//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.cpp
//
// Identification: src/storage/table/tuple.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

// 计算元组的大小：
// uint32_t tuple_size = schema->GetLength();
// 初始大小设为schema中固定长度的大小。
// 遍历所有不定长的列，并累加它们的长度和一个 uint32_t 大小（用于存储偏移量）。
// 如果值为 NULL，则长度设为0。
// 分配内存：
// data_.resize(tuple_size);
// 根据计算出的大小分配内存。
// std::fill(data_.begin(), data_.end(), 0);
// 将分配的内存初始化为0。
// 序列化每个属性：
// 遍历每一列，根据列的类型（内联或不内联）将值序列化到 data_ 中。
// 对于不内联的列，先存储相对偏移量，然后序列化实际数据。
// 对于内联列，直接在固定偏移位置序列化数据。
// TODO(Amadou): It does not look like nulls are supported. Add a null bitmap?
Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
  assert(values.size() == schema->GetColumnCount());

  // 1. Calculate the size of the tuple.
  uint32_t tuple_size = schema->GetLength();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetLength();
    if (len == BUSTUB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += (len + sizeof(uint32_t));
  }

  // 2. Allocate memory.
  data_.resize(tuple_size);
  std::fill(data_.begin(), data_.end(), 0);

  // 3. Serialize each attribute based on the input value.
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetLength();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
      // Serialize varchar value, in place (size+data).
      values[i].SerializeTo(data_.data() + offset);
      auto len = values[i].GetLength();
      if (len == BUSTUB_VALUE_NULL) {
        len = 0;
      }
      offset += (len + sizeof(uint32_t));
    } else {
      values[i].SerializeTo(data_.data() + col.GetOffset());
    }
  }
}

// 获取指定列的值。
// 通过 GetDataPtr 函数获取数据指针，然后调用 Value::DeserializeFrom 函数反序列化数据。
auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

// 功能说明：
// 从现有元组中提取出键值。
// 根据 key_attrs 中的索引，获取对应列的值并存储到 values 向量中。
// 使用这些值和键的schema构造一个新的元组。
auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs)
    -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

// 功能说明：
// 获取指定列的数据指针。
// 如果列是内联的，直接返回数据的指针。
// 如果列不是内联的，通过存储的偏移量获取实际数据的指针。
auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";

  return os.str();
}

void Tuple::SerializeTo(char *storage) const {
  int32_t sz = data_.size();
  memcpy(storage, &sz, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_.data(), sz);
}

void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
  this->data_.resize(size);
  memcpy(this->data_.data(), storage + sizeof(int32_t), size);
}

}  // namespace bustub
