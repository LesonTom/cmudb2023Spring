//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // frame_id_t start from 1
  is_accessible_.resize(num_frames + 1);
  curr_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // 1. Evict the history_list first
  *frame_id = 0;
  if (history_map_.empty() && cache_map_.empty()) {
    return false;
  }

  // history_list
  auto it = history_list_.end();
  while (it != history_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }

    history_map_.erase(*it);
    *frame_id = *it;
    use_count_[*it] = 0;
    is_accessible_[*it] = false;
    curr_size_--;
    history_list_.erase(it);
    return true;
  }

  // cache_list
  it = cache_list_.end();
  while (it != cache_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }
    *frame_id = *it;
    use_count_[*it] = 0;
    curr_size_--;
    is_accessible_[*it] = false;
    cache_list_.erase(it);
    cache_map_.erase(*it);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    // replacer_size_ is max frame_id
    throw std::exception();
  }

  use_count_[frame_id]++;
  // 1. if accesses reaches k times, put it to cache_list_
  if (use_count_[frame_id] == k_) {
    // If the frame exists in the history_list_, move it to the cache_list_.
    if (history_map_.count(frame_id) != 0U) {
      auto it = history_map_[frame_id];
      history_list_.erase(it);
    }
    history_map_.erase(frame_id);
    // 2. add frame_id into cache list
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (use_count_[frame_id] > k_) {
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
  } else {
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  // frame never appeared
  if (use_count_[frame_id] == 0) {
    return;
  }

  if (!is_accessible_[frame_id] && set_evictable) {
    // frame_id not visited and Mark the frame as eliminable
    curr_size_++;
  }

  if (is_accessible_[frame_id] && !set_evictable) {
    // frame_id is visited and Mark the frame as non-eliminable
    curr_size_--;
  }

  is_accessible_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (!is_accessible_[frame_id]) {
    return;
  }

  if (use_count_[frame_id] < k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  } else {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  }

  use_count_[frame_id] = 0;

  is_accessible_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
