#pragma once

#include <map>
#include "iterator.h"

namespace combotree {

class MapIterator : public Iterator {
 public:
  explicit MapIterator(std::map<uint64_t, uint64_t>* map)
      : map_(map), iter_(map->begin()) {}

  bool Begin() const { return iter_ == map_->begin(); }

  bool End() const { return iter_ == map_->end(); }

  void SeekToFirst() { iter_ = map_->begin(); }

  void SeekToLast() { iter_ = map_->end(); }

  void Seek(uint64_t target) {}

  void Next() { iter_++; }

  void Prev() { iter_--; }

  uint64_t key() const { return iter_->first; }

  uint64_t value() const { return iter_->second; }

 private:
  std::map<uint64_t, uint64_t>* map_;
  std::map<uint64_t, uint64_t>::iterator iter_;
};

} // namespace combotree