
#include <vector>

#if !defined(RMI_H)
#define RMI_H

namespace RMI
{
static const size_t desired_training_key_n = 10000000;

template <class key_t,  size_t root_error_bound>
class TwoStageRMI;

template <class key_t>
class LinearModel {
  typedef std::array<double, key_t::model_key_size()> model_key_t;
  template <class key_t_, size_t root_error_bound>
  friend class TwoStageRMI;

public:
    void prepare(const std::vector<key_t> &keys,
                const std::vector<size_t> &positions);
    void prepare(const typename std::vector<key_t>::const_iterator &keys_begin,
                uint32_t size);
    void prepare_model(const std::vector<double *> &model_key_ptrs,
                        const std::vector<size_t> &positions);
    size_t predict(const key_t &key) const;
    size_t get_error_bound(const std::vector<key_t> &keys,
                            const std::vector<size_t> &positions);
    size_t get_error_bound(
        const typename std::vector<key_t>::const_iterator &keys_begin,
        uint32_t size);

private:
    std::array<double, key_t::model_key_size() + 1> weights;
};


template <class key_t, size_t root_error_bound = 32>
class TwoStageRMI {

    typedef LinearModel<key_t> linear_model_t;
    const double root_memory_constraint = 1024 * 1024;
public:
    TwoStageRMI() {}

    TwoStageRMI(const std::vector<key_t> &keys) { init(keys); }
    ~TwoStageRMI();
    void init(const std::vector<key_t> &keys);
    size_t predict(const key_t &key);;
    size_t rmi_model_n() { return rmi_2nd_stage_model_n;}
    // void calculate_err(const std::vector<key_t> &keys,
    //                  const std::vector<val_t> &vals, size_t group_n_trial,
    //                  double &err_at_percentile, double &max_err,
    //                  double &avg_err);
private:
    void adjust_rmi(const std::vector<key_t> &train_keys);
    void train_rmi(const std::vector<key_t> &train_keys, size_t rmi_2nd_stage_model_n);

    size_t pick_next_stage_model(size_t pos_pred);

    linear_model_t rmi_1st_stage;
    linear_model_t *rmi_2nd_stage = nullptr;
    size_t rmi_2nd_stage_model_n = 0;
    size_t keys_n = 0;
};

class Key_64 {
  typedef std::array<double, 1> model_key_t;

 public:
  static constexpr size_t model_key_size() { return 1; }
  static Key_64 max() {
    static Key_64 max_key(std::numeric_limits<uint64_t>::max());
    return max_key;
  }
  static Key_64 min() {
    static Key_64 min_key(std::numeric_limits<uint64_t>::min());
    return min_key;
  }

  Key_64() : key(0) {}
  Key_64(uint64_t key) : key(key) {}
  Key_64(const Key_64 &other) { key = other.key; }
  Key_64 &operator=(const Key_64 &other) {
    key = other.key;
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = key;
    return model_key;
  }

  friend bool operator<(const Key_64 &l, const Key_64 &r) { return l.key < r.key; }
  friend bool operator>(const Key_64 &l, const Key_64 &r) { return l.key > r.key; }
  friend bool operator>=(const Key_64 &l, const Key_64 &r) { return l.key >= r.key; }
  friend bool operator<=(const Key_64 &l, const Key_64 &r) { return l.key <= r.key; }
  friend bool operator==(const Key_64 &l, const Key_64 &r) { return l.key == r.key; }
  friend bool operator!=(const Key_64 &l, const Key_64 &r) { return l.key != r.key; }

  uint64_t key;
};

} // namespace RMI

#endif