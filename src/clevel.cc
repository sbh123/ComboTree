#include <iostream>
#include "clevel.h"

namespace combotree {

// TODO: flush and fence
// always success (if no exception)
CLevel::Node* CLevel::Node::Put(MemControl* mem, uint64_t key, uint64_t value, Node* parent) {
  if (type == Type::LEAF) {

    bool exist;
    int pos = leaf_buf.Find(key, exist);
    if (exist) {
      *(uint64_t*)leaf_buf.pvalue(pos) = value;
      flush(leaf_buf.pvalue(pos));
      fence();
      return this;
    }

    if (leaf_buf.entries == leaf_buf.max_entries) {
      // split
      Node* new_node = mem->NewNode(Type::LEAF, leaf_buf.suffix_bytes);
      // MoveData(dest, start_pos, entry_count)
      leaf_buf.MoveData(&new_node->leaf_buf, leaf_buf.entries/2, leaf_buf.entries-leaf_buf.entries/2);
      // set next pointer
      memcpy(new_node->next, next, sizeof(next));
      SetNext(mem->BaseAddr(), new_node);

      // insert new pair
      if (key > new_node->leaf_buf.key(0, key))
        new_node->Put(mem, key, value, nullptr);
      else
        Put(mem, key, value, nullptr);

      if (parent == nullptr) {
        Node* new_root = mem->NewNode(Type::INDEX, leaf_buf.suffix_bytes);
        uint64_t tmp = (uint64_t)this - mem->BaseAddr();
        memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
        new_root->index_buf.Put(0, new_node->leaf_buf.pkey(0),
                                (uint64_t)new_node-mem->BaseAddr());
        return new_root;
      } else {
        // store middle key in new_node.buf.key[entries] temporally
        memcpy(new_node->leaf_buf.pkey(new_node->leaf_buf.entries),
               new_node->leaf_buf.pkey(0), leaf_buf.suffix_bytes);
        return new_node;
      }
    } else {
      leaf_buf.Put(pos, key, value);
      return this;
    }

  } else if (type == Type::INDEX) {

    bool exist;
    int pos = index_buf.Find(key, exist);
    Node* child = GetChild(pos, mem->BaseAddr());
    Node* new_node = child->Put(mem, key, value, this);
    if (new_node != child) {
      // child has been split
      // new key is stored in buf.key[entires] temporally
      index_buf.Put(pos, new_node->index_buf.pkey(new_node->index_buf.entries),
                    (uint64_t)new_node-mem->BaseAddr());

      if (index_buf.entries == index_buf.max_entries) {
        // full, split
        Node* new_node = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
        // MoveData(dest, start_pos, entry_count)
        index_buf.MoveData(&new_node->index_buf, (index_buf.entries+1)/2, index_buf.entries/2);
        memcpy(new_node->first_child, index_buf.pvalue(index_buf.entries-1),
               sizeof(new_node->first_child));

        if (parent == nullptr) {
          Node* new_root = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
          uint64_t tmp = (uint64_t)this - mem->BaseAddr();
          memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
          new_root->index_buf.Put(0, index_buf.pkey(index_buf.entries-1),
                                  (uint64_t)new_node-mem->BaseAddr());
          index_buf.entries--;
          return new_root;
        } else {
          // store middle key in new_node.buf.key[entries] temporally
          memcpy(new_node->index_buf.pkey(new_node->index_buf.entries),
                index_buf.pkey(index_buf.entries-1), index_buf.suffix_bytes);
          index_buf.entries--;
          return new_node;
        }
      } else {
        return this;
      }
    } else {
      return this;
    }

  } else {
    assert(0);
    return this;
  }
}

bool CLevel::Node::Get(MemControl* mem, uint64_t key, uint64_t& value) const {
  const Node* leaf = FindLeaf(mem, key);
  bool exist;
  int pos = leaf->leaf_buf.Find(key, exist);
  if (exist) {
    value = leaf->leaf_buf.value(pos);
    return true;
  } else {
    return false;
  }
}

bool CLevel::Node::Delete(MemControl* mem, uint64_t key, uint64_t* value) {
  Node* leaf = (Node*)FindLeaf(mem, key);
  bool exist;
  int pos = leaf->leaf_buf.Find(key, exist);
  if (exist && value)
    *value = leaf->leaf_buf.value(pos);
  return exist ? leaf->leaf_buf.Delete(pos) : true;
}


/******************** CLevel ********************/
CLevel::CLevel()
{
  // set root to NULL: set LSB to 1
  root_[0] = 1;
}

void CLevel::Setup(MemControl* mem, int suffix_len) {
  uint64_t new_root = (uint64_t)mem->NewNode(Node::Type::LEAF, suffix_len);
  // set next to NULL: set LSB to 1
  ((Node*)new_root)->next[0] |= 1;
  new_root -= mem->BaseAddr();
  memcpy(root_, &new_root, sizeof(root_));
  flush(&root_);
  fence();
}

void CLevel::Setup(MemControl* mem, KVBuffer<48+64,8>& blevel_buf) {
  Node* new_root = mem->NewNode(Node::Type::LEAF, blevel_buf.suffix_bytes);
  blevel_buf.MoveData(&new_root->leaf_buf, 0, blevel_buf.entries);

  // set next to NULL: set LSB to 1
  new_root->next[0] |= 1;
  new_root = (Node*)((uint64_t)new_root - mem->BaseAddr());
  memcpy(root_, &new_root, sizeof(root_));
  flush(&root_);
  fence();
}

bool CLevel::Put(MemControl* mem, uint64_t key, uint64_t value) {
  Node* old_root = root(mem->BaseAddr());
  Node* new_root = old_root->Put(mem, key, value, nullptr);
  if (old_root != new_root) {
    new_root = (Node*)((uint64_t)new_root - mem->BaseAddr());
    memcpy(root_, &new_root, sizeof(root_));
    flush(&root_);
    fence();
  }
  return true;
}

} // namespace combotree