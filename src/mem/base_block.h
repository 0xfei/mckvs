//
// Created by hhx on 2018/9/13.
//

#ifndef TBKVS_BASE_BLOCK_H
#define TBKVS_BASE_BLOCK_H


#include <cstdint>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "common/rwlock.h"


namespace MCKVS
{

using BLOCK_KEY = std::string;
using TABLE_KEY = std::vector<BLOCK_KEY>;

enum BLOCK_TYPE {
    BASE_BLOCK   = 0,
    BASE_VALUE   = 1,
    VECT_VALUE   = 2,
};

class BlockInterface;
using BlockPtr = std::shared_ptr<BlockInterface>;

struct KVChain {
    int level;
    TABLE_KEY key;
    BlockPtr value;
};


/*
 * Common interface
 */
class BlockInterface {
public:
    BlockInterface(): valid_(true) {}
    BlockInterface(BLOCK_TYPE t): valid_(true), type_(t) {}
    BlockInterface(BLOCK_KEY k, BLOCK_TYPE t): key_(k), type_(t), valid_(true) {}
    virtual ~BlockInterface() {}

    bool IsBaseBlock() { return type_ == BASE_BLOCK; }

    virtual void Mark() { std::atomic_store(&valid_, false); }
    virtual void Unmark() { std::atomic_store(&valid_, true); }
    virtual bool Valid() { return std::atomic_load(&valid_); }

    virtual void* GetValue() { return NULL; }

    virtual void GetLeafBlock(std::vector<BlockPtr>& blocks) {}
    virtual bool InsertBlock(const KVChain& kv_chain) { return true; }

    virtual BlockPtr Get(BLOCK_KEY key) { return NULL; }
    virtual bool Insert(BLOCK_KEY key, BlockPtr block) { return true; }
    virtual bool Remove(BLOCK_KEY key) { return true; }
    virtual bool Clear() { return true; }

    virtual void Acquire() { std::atomic_fetch_add(&used_, 1); }
    virtual void Release() { std::atomic_fetch_sub(&used_, 1); }
    virtual bool Freedom() { return std::atomic_load(&used_) == 0; }
    virtual void Wait() {
        int t = 0;
        std::chrono::milliseconds ms(1);
        while (!std::atomic_compare_exchange_strong(&used_, &t, 0)) {
            std::this_thread::sleep_for(ms);
        }
    }

private:
    BLOCK_TYPE type_;
    std::atomic<int> used_;
    BLOCK_KEY key_;

protected:
    std::atomic<bool> valid_;
    RWLock rwlock_;
};


/*
 * acquire helper
 */
class AcquireHelper {
public:
    AcquireHelper(BlockInterface* block): block_(block) { block_->Acquire(); }
    virtual ~AcquireHelper() { block_->Release(); }

private:
    BlockInterface* block_;
};


/*
 * BaseBlock, used for normal mem
 */
class BaseBlock : public BlockInterface {
public:
    BaseBlock(): BlockInterface(BASE_BLOCK) {}
    BaseBlock(BLOCK_KEY key): BlockInterface(key, BASE_BLOCK) {}
    virtual ~BaseBlock() {}

    virtual void Mark() override { Marking(false); }
    virtual void Unmark() override { Marking(true); }

    virtual bool Remove(BLOCK_KEY key) override {
        RWLockGuard<RWLock> rw(rwlock_, true);
        if (Valid()) sub_key_.erase(key);
        return true;
    }

    virtual bool Clear() override {
        if (Valid()) Mark();
        if (!Freedom()) { Wait(); }
        if (sub_key_.empty()) return true;
        for (auto & k : sub_key_) {
            if (!k.second->Freedom()) { k.second->Wait(); }
            k.second->Clear();
        }
        sub_key_.clear();
        return true;
    }

    virtual bool Insert(BLOCK_KEY key, BlockPtr block) override {
        RWLockGuard<RWLock> rw(rwlock_, true);
        AcquireHelper aq(this);
        sub_key_[key] = block;
        return true;
    }

    virtual BlockPtr Get(BLOCK_KEY key) override {
        RWLockGuard<RWLock> rw(rwlock_, false);
        AcquireHelper aq(this);
        if (!Valid() || sub_key_.find(key) == sub_key_.end()) return NULL;
        if (!sub_key_[key]->Valid()) return NULL;
        return sub_key_[key];
    }

    virtual void GetLeafBlock(std::vector<BlockPtr>& blocks) override {
        RWLockGuard<RWLock> rw(rwlock_, false);
        AcquireHelper aq(this);
        for (auto & k : sub_key_) {
            k.second->Acquire();
            if (k.second->Valid()) {
                if (!k.second->IsBaseBlock()) {
                    blocks.push_back(k.second);
                } else {
                    k.second->GetLeafBlock(blocks);
                }
            }
            k.second->Release();
        }
    }

    virtual bool InsertBlock(const KVChain& kv_chain) override {
        RWLockGuard<RWLock> rw(rwlock_, true);
        AcquireHelper aq(this);
        if (kv_chain.key.empty()) return false;
        if (kv_chain.key.size() == 1) {
            sub_key_[kv_chain.key[0]] = kv_chain.value;
            return true;
        }
        if (sub_key_.find(kv_chain.key[0]) == sub_key_.end()) {
            sub_key_[kv_chain.key[0]] = std::make_shared<BaseBlock>();
        }
        return sub_key_[kv_chain.key[0]]->InsertBlock({
            kv_chain.level-1,
            {kv_chain.key.begin()+1, kv_chain.key.end()},
            kv_chain.value});
    }

private:
    void Marking(bool valid) {
        RWLockGuard<RWLock> rw(rwlock_, true);
        std::atomic_store(&valid_, valid);
        for (auto & k : sub_key_) {
            valid ? k.second->Unmark() : k.second->Mark();
        }
    }

    std::unordered_map<BLOCK_KEY, BlockPtr> sub_key_;
};


/*
 * simple kv-store
 */
template<typename P_>
class StringValue : public BlockInterface {
public:
    StringValue(const P_& s) : value_(s), BlockInterface(BASE_VALUE) {}
    virtual ~StringValue() {}

    bool GetValue(P_& v) { v = value_; return true; }
private:
    P_ value_;
};


/*
 * Sample vector leaf
 */
template<typename P_>
class VectorBlock : public BlockInterface {
public:
    VectorBlock() : BlockInterface(VECT_VALUE) {}
    virtual ~VectorBlock() {}

    bool GetValue(std::vector<P_>& v) {
        v = value_;
        return true;
    }

    void PushBack(const P_ &s) { value_.push_back(s); }

private:
    std::vector<P_> value_;
};


}

#endif //TBKVS_BASE_BLOCK_H
