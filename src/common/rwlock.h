//
// Created by hhx on 2018/9/13.
//

#ifndef TBKVS_RWLOCK_H
#define TBKVS_RWLOCK_H

#include <mutex>

namespace MCKVS
{

class RWLock {
public:
    void RLock() {
        std::unique_lock<std::mutex> mut(mutex_);
        rcond_.wait(mut, [=]() -> bool { return !wn_; });
        std::atomic_fetch_add(&rn_, 1);
    }

    void RUnlock() {
        std::unique_lock<std::mutex> mut(mutex_);
        std::atomic_fetch_sub(&rn_, 1);
        if (rn_ == 0 && wn_ > 0) {
            wcond_.notify_one();
        }
    }

    void WLock() {
        std::unique_lock<std::mutex> mut(mutex_);
        std::atomic_fetch_add(&wn_, 1);
        wcond_.wait(mut, [=]() -> bool { return !rn_ && !wf_; });
        wf_ = 1;
    }

    void WUnlock() {
        std::unique_lock<std::mutex> mut(mutex_);
        std::atomic_fetch_sub(&wn_, 1);
        wf_ = 0;
        if (wn_ == 0) {
            rcond_.notify_all();
        } else {
            wcond_.notify_one();
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable rcond_, wcond_;
    volatile std::atomic<int> rn_{0}, wn_{0}, wf_{0};
};

template<typename _RWLock>
class RWLockGuard {
public:
    explicit RWLockGuard(_RWLock &rwlock, bool write) :
        rwlock_(rwlock), write_(write), locked_(true) {
        write_ ? rwlock_.WLock() : rwlock_.RLock();
    }
    ~RWLockGuard() {
        if (locked_) {
            write_ ? rwlock_.WUnlock() : rwlock_.RUnlock();
        }
    }
    void Unlock() {
        locked_ = false;
        write_ ? rwlock_.WUnlock() : rwlock_.RUnlock();
    }
private:
    _RWLock &rwlock_;
    bool write_;
    bool locked_;
};

}

#endif //TBKVS_RWLOCK_H
