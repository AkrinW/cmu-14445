#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto node = root_;
  for (auto ch: key) {
    if (node == nullptr || !node->children_.count(ch)) {
      return nullptr;
    }
    node = node->children_.at(ch);
  }
  const auto *value_node = dynamic_cast<const TrieNodeWithValue<T>*>(node.get());
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();

  // throw NotImplementedException("Trie::Get is not implemented.");
  // You should walk through the trie to find the corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // std::cout << 1;
  std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
  // std::shared_ptr<T> val_p = nullptr;
  if (key.size() == 0) {
    std::shared_ptr<bustub::TrieNodeWithValue<T>> newRoot = nullptr;
    // 如果根节点无子节点
    if (root_ == nullptr || root_->children_.empty()) {
      // 直接修改根节点
      newRoot = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
    } else {
      // 如果有，构造一个新节点，children指向root_的children
      newRoot = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }
    // 返回新的Trie
    return Trie(std::move(newRoot));
  }
  std::shared_ptr<TrieNode> newRoot = nullptr;
  if (root_ == nullptr) {
    newRoot = std::make_shared<TrieNode>();
  } else {
    newRoot = root_->Clone();
    // newRoot = root_->Clone();
  }
  auto node = newRoot;
  for (size_t i = 0; i < key.size(); ++i) {
    if (i == key.size() - 1) {
      if (node->children_.count(key[i])) {
        auto nextNode = node->children_[key[i]];
        auto newNode = std::make_shared<TrieNodeWithValue<T>>(nextNode->children_, std::move(val_p));
        node->children_[key[i]] = newNode;
      } else {
        auto newNode = std::make_shared<TrieNodeWithValue<T>>(std::move(val_p));
        node->children_[key[i]] = newNode;
      }
    } else {
      if (node->children_.count(key[i])) {
        auto nextNode = node->children_[key[i]];
        if (nextNode->is_value_node_) {
          auto newNode = std::make_shared<TrieNodeWithValue<T>>(std::move(nextNode));
          node->children_[key[i]] = newNode;
          node = newNode;
        } else {
          auto newNode = std::make_shared<TrieNode>(std::move(nextNode));
          node->children_[key[i]] = newNode;
          node = newNode;
        }
        // auto newNode = std::make_shared<TrieNode>(std::move(node->children_[key[i]]));
        // node->children_[key[i]] = 
      } else {
        auto newNode = std::make_shared<TrieNode>();
        node->children_[key[i]] = newNode;
        node = newNode;
      }
    }
  }
  // for (auto ch: key) {
  //   auto it = node->children_.find(ch);
  //   if (node != newRoot) {
  //     parent = node;
  //   }
  //   if (it == node->children_.end()) {
  //     auto newNode = std::make_shared<TrieNode>();
  //     node->children_[ch] = newNode;
  //     node = newNode;
  //   } else {
  //     auto newNode = it->second->Clone();
  //     // std::make_shared<TrieNode>(*it->second);
  //     node->children_[ch] = newNode;
  //     node = newNode;
  //   }
  // }
  // auto newValueNode = std::make_shared<TrieNodeWithValue<T>>(node->children_, val_p);
  // parent->children_[key.back()] = newValueNode;
  return Trie(std::move(newRoot));

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
