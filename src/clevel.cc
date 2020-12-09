#include <iostream>
#include "combotree_config.h"
#include "clevel.h"

namespace combotree {

int CLevel::MemControl::file_id_ = 0;

#ifndef BUF_SORT
void CLevel::Node::PutChild(MemControl* mem, void* key, const Node* child) {
  assert(type == Type::INDEX);
  assert(index_buf.entries < index_buf.max_entries);
  index_buf.Put(index_buf.entries, key, (uint64_t)child - mem->BaseAddr());
}
#endif

// TODO: flush and fence
// always success (if no exception)
// FIXME: return false when exist
CLevel::Node* CLevel::Node::Put(MemControl* mem, uint64_t key, uint64_t value, Node* parent) {
#ifdef BUF_SORT
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
      leaf_buf.MoveData(&new_node->leaf_buf, leaf_buf.entries/2);
      // set next pointer
      memcpy(new_node->next, next, sizeof(next));
      SetNext(mem->BaseAddr(), new_node);

      // insert new pair, Put will do fence
      if (key > new_node->leaf_buf.key(0, key)) {
        flush(this);
        new_node->Put(mem, key, value, nullptr);
      } else {
        flush(new_node);
        flush((uint8_t*)new_node+64);
        Put(mem, key, value, nullptr);
      }

      if (parent == nullptr) {
        Node* new_root = mem->NewNode(Type::INDEX, leaf_buf.suffix_bytes);
        uint64_t tmp = (uint64_t)this - mem->BaseAddr();
        // set first_child before Put, beacause Put will do flush,
        // which contains first_child
        memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
        new_root->index_buf.Put(0, new_node->leaf_buf.pkey(0),
                                (uint64_t)new_node - mem->BaseAddr());
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
        index_buf.MoveData(&new_node->index_buf, (index_buf.entries+1)/2);
        memcpy(new_node->first_child, index_buf.pvalue(index_buf.entries-1),
               sizeof(new_node->first_child));
        flush(this);
        flush(new_node);
        flush((uint8_t*)new_node+64);
        fence();

        if (parent == nullptr) {
          Node* new_root = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
          uint64_t tmp = (uint64_t)this - mem->BaseAddr();
          memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
          index_buf.entries--;
          flush(this);
          new_root->index_buf.Put(0, index_buf.pkey(index_buf.entries),
                                  (uint64_t)new_node - mem->BaseAddr());
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
#else
  if (type == Type::LEAF) {

    bool exist;
    int pos = leaf_buf.Find(key, exist);
    if (exist) {
      *(uint64_t*)leaf_buf.pvalue(pos) = value;
      flush(leaf_buf.pvalue(pos));
      fence();
      return this;
    }

    leaf_buf.Put(pos, key, value);
    if (leaf_buf.entries == leaf_buf.max_entries) {
      // split
      Node* new_node = mem->NewNode(Type::LEAF, leaf_buf.suffix_bytes);
      // get sorted index
      int sorted_index[32];
      leaf_buf.GetSortedIndex(sorted_index);
      // MoveData(dest, start_pos, entry_count)
      leaf_buf.CopyData(&new_node->leaf_buf, leaf_buf.entries/2, sorted_index);
      // set next pointer
      memcpy(new_node->next, next, sizeof(next));
      // persist new node
      flush(new_node);
      flush((uint8_t*)new_node+64);
      fence();

      if (parent == nullptr) {
        Node* new_root = mem->NewNode(Type::INDEX, leaf_buf.suffix_bytes);
        uint64_t tmp = (uint64_t)this - mem->BaseAddr();
        // set first_child before Put, beacause Put will do flush,
        // which contains first_child
        memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
        // new_node is sorted now, so key(0) is the node key
        new_root->PutChild(mem, new_node->leaf_buf.pkey(0), new_node);
        // flush root
        flush(new_root);
        flush((uint8_t*)new_root+64);
        fence();

        SetNext(mem->BaseAddr(), new_node);
        leaf_buf.DeleteData(leaf_buf.entries/2, sorted_index);
        return new_root;
      } else {
        // new_node is sorted now, so key(0) is the node key
        parent->PutChild(mem, new_node->leaf_buf.pkey(0), new_node);
        SetNext(mem->BaseAddr(), new_node);
        leaf_buf.DeleteData(leaf_buf.entries/2, sorted_index);
        return new_node;
      }
    } else {
      return this;
    }

  } else if (type == Type::INDEX) {

    bool exist;
    int pos = index_buf.FindLE(key, exist);
    Node* child = GetChild(pos+1, mem->BaseAddr());
    Node* new_child = child->Put(mem, key, value, this);
    if (new_child != child) {
      if (index_buf.entries == index_buf.max_entries) {
        // full, split
        Node* new_node = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
        int sorted_index[32];
        index_buf.GetSortedIndex(sorted_index);
        // copy data to new_node
        index_buf.CopyData(&new_node->index_buf, (index_buf.entries+1)/2, sorted_index);
        // set new_node.first_child
        memcpy(new_node->first_child, index_buf.pvalue(sorted_index[(index_buf.entries+1)/2-1]), sizeof(new_node->first_child));
        // persist new_node
        flush(new_node);
        flush((uint8_t*)new_node+64);
        fence();

        if (parent == nullptr) {
          Node* new_root = mem->NewNode(Type::INDEX, index_buf.suffix_bytes);
          uint64_t tmp = (uint64_t)this - mem->BaseAddr();
          memcpy(new_root->first_child, &tmp, sizeof(new_root->first_child));
          new_root->PutChild(mem, index_buf.pkey(sorted_index[(index_buf.entries+1)/2-1]), new_node);
          // flush root
          flush(new_root);
          flush((uint8_t*)new_root+64);
          fence();

          index_buf.DeleteData((index_buf.entries+1)/2-1, sorted_index);
          return new_root;
        } else {
          parent->PutChild(mem, index_buf.pkey(sorted_index[(index_buf.entries+1)/2-1]), new_node);
          index_buf.DeleteData((index_buf.entries+1)/2-1, sorted_index);
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
#endif // BUF_SORT
}

bool CLevel::Node::Update(MemControl* mem, uint64_t key, uint64_t value) {
  Node* leaf = (Node*)FindLeaf(mem, key);
  bool exist;
  int pos = leaf->leaf_buf.Find(key, exist);
  if (exist)
    return leaf->leaf_buf.Update(pos, value);
  else
    assert(0);
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
  ((Node*)new_root)->next[0] = 1;
  new_root -= mem->BaseAddr();
  memcpy(root_, &new_root, sizeof(root_));
  flush(&root_);
  fence();
}

void CLevel::Setup(MemControl* mem, KVBuffer<48+64,8>& blevel_buf) {
  Node* new_root = mem->NewNode(Node::Type::LEAF, blevel_buf.suffix_bytes);
  memcpy(&new_root->leaf_buf, &blevel_buf, sizeof(blevel_buf));
  flush(new_root);
  flush((uint8_t*)new_root+64);

  // set next to NULL: set LSB to 1
  new_root->next[0] = 1;
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