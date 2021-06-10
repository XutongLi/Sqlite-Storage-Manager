/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 1. if txn is not in growing phase, betray the rule of 2PL
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. get list of the record
  std::unique_lock<std::mutex> table_latch(mutex_);
  TxListForRecord &tx_list_for_record = lock_table_[rid];
  std::unique_lock<std::mutex> list_latch(tx_list_for_record.mutex_);
  table_latch.unlock();
  // 3. whether txn can be granted lock of the record
  bool can_granted = false;
  if (tx_list_for_record.locks_.empty()) {
    can_granted = true;
  } else {
    TxLockForRecord &last_lock = tx_list_for_record.locks_.back();
    if (last_lock.granted_ && last_lock.lock_type_ == LockType::SHARED)
      can_granted = true;
  }
  // 4. if txn has lower priority, abort; otherwise wait
  if (!can_granted && txn->GetTransactionId() > tx_list_for_record.locks_.back().txn_id_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 5. insert lock request in the list
  tx_list_for_record.locks_.emplace_back(txn->GetTransactionId(), LockType::SHARED, can_granted);
  TxLockForRecord &last_lock = tx_list_for_record.locks_.back();
  if (!can_granted) {
    list_latch.unlock();
    last_lock.Wait();
  }
  txn->GetSharedLockSet()->insert(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 1. if txn is not in growing phase, betray the rule of 2PL
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. get list of the record
  std::unique_lock<std::mutex> table_latch(mutex_);
  TxListForRecord &tx_list_for_record = lock_table_[rid];
  std::unique_lock<std::mutex> list_latch(tx_list_for_record.mutex_);
  table_latch.unlock();
  // 3. whether txn can be granted lock of the record
  bool can_granted = tx_list_for_record.locks_.empty();
  // 4. if txn has lower priority, abort; otherwise wait
  if (!can_granted && txn->GetTransactionId() > tx_list_for_record.locks_.back().txn_id_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 5. insert lock request in the list
  tx_list_for_record.locks_.emplace_back(txn->GetTransactionId(), LockType::EXCLUSIVE, can_granted);
  TxLockForRecord &last_lock = tx_list_for_record.locks_.back();
  if (!can_granted) {
    list_latch.unlock();
    last_lock.Wait();
  }
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 1. if txn is not in growing phase, betray the rule of 2PL
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. get list of the record
  std::unique_lock<std::mutex> table_latch(mutex_);
  TxListForRecord &tx_list_for_record = lock_table_[rid];
  std::unique_lock<std::mutex> list_latch(tx_list_for_record.mutex_);
  table_latch.unlock();
  // 3. whether the upgrage could be executed
  if (tx_list_for_record.has_upgrated_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  auto it = find_if(tx_list_for_record.locks_.begin(), tx_list_for_record.locks_.end(),
                    [txn](const TxLockForRecord &tx_lock) {
                      return tx_lock.txn_id_ == txn->GetTransactionId();
                    });
  if (it == tx_list_for_record.locks_.end() || it->lock_type_ != LockType::SHARED || !it->granted_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  tx_list_for_record.locks_.erase(it);
  txn->GetSharedLockSet()->erase(rid);
  // 4. whether txn can be granted lock of the record
  bool can_granted = tx_list_for_record.locks_.empty();
  // 5. if txn has lower priority, abort; otherwise wait
  if (!can_granted && txn->GetTransactionId() > tx_list_for_record.locks_.back().txn_id_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 6. insert lock request in the list
  if (can_granted)  
    tx_list_for_record.locks_.emplace_back(txn->GetTransactionId(), LockType::EXCLUSIVE, can_granted);
  else
    tx_list_for_record.locks_.emplace_back(txn->GetTransactionId(), LockType::UPGRADING, can_granted);
  TxLockForRecord &last_lock = tx_list_for_record.locks_.back();
  if (!can_granted) {
    tx_list_for_record.has_upgrated_ = true;
    list_latch.unlock();
    last_lock.Wait();
  }
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 1. for S2PL, lock should be released after txn committing
  if (strict_2PL_) {
    if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
      // otherwise, abort it
      txn->SetState(TransactionState::ABORTED);
      return false;
    } 
  // 2. for 2PL, once there is one lock is release, state transfers to shrinking
  } else if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 3. get lock list of the record
  std::unique_lock<std::mutex> table_latch(mutex_);
  TxListForRecord &tx_list_for_record = lock_table_[rid];
  std::unique_lock<std::mutex> list_latch(tx_list_for_record.mutex_);
  // 4. find the lock in the list
  auto it = find_if(tx_list_for_record.locks_.begin(), tx_list_for_record.locks_.end(),
                    [txn](const TxLockForRecord &tx_lock) {
                      return tx_lock.txn_id_ == txn->GetTransactionId();
                    });
  // 5. remove the lock
  if (it->lock_type_ == LockType::SHARED)
    txn->GetSharedLockSet()->erase(rid);
  else
    txn->GetExclusiveLockSet()->erase(rid);
  tx_list_for_record.locks_.erase(it);
  if (tx_list_for_record.locks_.empty()) {
    lock_table_.erase(rid);
    return true;
  }
  table_latch.unlock();
  // 6. grant lock to next txn
  for (auto &tx_lock : tx_list_for_record.locks_) {
    if (tx_lock.granted_)
      break;
    tx_lock.Grant();
    if (tx_lock.lock_type_ == LockType::SHARED)
      continue;
    if (tx_lock.lock_type_ == LockType::UPGRADING) {
      tx_list_for_record.has_upgrated_ = false;
      tx_lock.lock_type_ = LockType::EXCLUSIVE;
    }
    break;
  }
  return true;
}

} // namespace cmudb
