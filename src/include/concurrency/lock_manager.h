/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "index/b_plus_tree.h"

namespace cmudb {

class LockManager {

  /*
   * lock requested from txn to manager for one record 
   */ 
  struct TxLockForRecord {
    TxLockForRecord(txn_id_t txn_id, LockType lock_type, bool granted) 
                : txn_id_(txn_id), lock_type_(lock_type), granted_(granted) {}
    
    void Wait() {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return this->granted_; });
    }

    void Grant() {
      std::unique_lock<std::mutex> lock(mutex_);
      granted_ = true;
      cv_.notify_one();
    }

    txn_id_t txn_id_;
    LockType lock_type_;
    bool granted_;
    std::mutex mutex_;
    std::condition_variable cv_;
  };
  
  /*
   * txn lock request list for one record 
   */ 
  struct TxListForRecord {
    std::list<TxLockForRecord> locks_;
    std::mutex mutex_;
    bool has_upgrated_;
  };

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  

  bool strict_2PL_;
  std::unordered_map<RID, TxListForRecord> lock_table_;
  std::mutex mutex_;    // for lock_table
};

} // namespace cmudb
