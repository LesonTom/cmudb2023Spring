#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  auto cur = root_;
  for (auto ch : key) {
    if (cur->children_.count(ch) == 0U) {
      return nullptr;
    }
    auto temp = cur->children_.at(ch);
    cur = temp;
  }

  if (cur->is_value_node_ == false) {
    return nullptr;
  }

  auto temp_value = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (temp_value == nullptr) return nullptr;

  return temp_value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  Trie trie;
  if (root_ == nullptr) {
    std::shared_ptr<const TrieNode> root = std::make_shared<const TrieNode>();
    return Trie(root).Put(key, std::move(value));
  }

  // Copy root_ to ptr, use ptr and return root (ptr == root)
  std::shared_ptr<TrieNode> ptr = root_->Clone();
  auto root = ptr;

  // if key is NULL
  if (key.empty()) {
    ptr = std::make_shared<TrieNodeWithValue<T>>(ptr->children_, std::make_shared<T>(std::move(value)));
    return Trie(ptr);
  }

  // traverse key
  size_t i = 0;
  for (; i < key.size() - 1; i++) {
    if (ptr->children_.count(key[i]) == 0) {
      // branch is null, create new branch
      std::shared_ptr<TrieNode> newNode = std::make_shared<TrieNode>();
      ptr->children_[key[i]] = newNode;
      ptr = newNode;
    } else {
      // branch is existing, access it
      std::shared_ptr<TrieNode> child = ptr->children_[key[i]].get()->Clone();
      ptr->children_[key[i]] = child;
      ptr = child;
    }
  }

  // handle the latest character
  std::shared_ptr<TrieNodeWithValue<T>> valueNode;
  if (ptr->children_.count(key.back()) == 0) {
    // not exist
    valueNode = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    valueNode = std::make_shared<TrieNodeWithValue<T>>(ptr->children_[key.back()]->children_,
                                                       std::make_shared<T>(std::move(value)));
  }
  ptr->children_[key.back()] = valueNode;
  return Trie(root);
}

std::shared_ptr<const TrieNode> Trie::Dfs(const std::shared_ptr<const TrieNode> &root, std::string_view key,
                                          size_t index) const {
  if (index == key.size()) {
    if (root->children_.empty()) {
      return nullptr;
    }
    /*
     * auto ttt = root->Clone();
     * ttt->is_value_node_ = false;
     * return ttt;
     * 是value没有清空吗？
     * */
    return std::make_shared<const TrieNode>(root->children_);
  }
  std::shared_ptr<const TrieNode> new_node;
  auto t = std::const_pointer_cast<TrieNode>(root);
  if (t->children_.find(key[index]) != root->children_.end()) {
    /*如果这个key存在，则递归去处理子节点*/
    new_node = Dfs(t->children_[key[index]], key, index + 1);
    auto node = root->Clone();
    /*获得处理之后的子节点，如果子节点为空了，则需要再字典树中把这个分支清理掉*/
    if (new_node) {
      node->children_[key[index]] = new_node;
    } else {
      node->children_.erase(key[index]);
      if (!node->is_value_node_ && node->children_.empty()) {
        return nullptr;
      }
    }
    return node;
  }
  /*当这个key不存在时，直接返回原来的节点即可*/
  return root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto root = Dfs(root_, key, 0);
  return Trie(root);

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
