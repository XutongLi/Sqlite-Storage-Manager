/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    if (mp.find(value) != mp.end())
        used.erase(mp[value]);
    used.push_front(value);
    mp[value] = used.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    if (mp.empty())
        return false;
    value = used.back();
    mp.erase(used.back());
    used.pop_back();
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    if (mp.find(value) == mp.end())
        return false;
    used.erase(mp[value]);
    mp.erase(value);
    return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return mp.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
