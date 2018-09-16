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

namespace MCKVS
{

using BLOCK_KEY = std::string;
using TABLE_KEY = std::vector<BLOCK_KEY>;

enum BLOCK_TYPE {
    BASE_BLOCK   = 0,
    BASE_VALUE   = 1,
    VECT_VALUE   = 2,
    UDEF_VALUE   = 3,
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
    void SetType(BLOCK_TYPE t) { type_ = t; }

    virtual void SetInvalid() { std::atomic_store(&valid_, false); }
    virtual void SetValid() { std::atomic_store(&valid_, true); }
    virtual bool Valid() { return std::atomic_load(&valid_); }

    virtual BlockPtr GetValue() { return value_; }
	void SetValue(BlockPtr v) { value_ = v; }

	virtual BlockPtr Get(BLOCK_KEY key) { return NULL; }
    virtual int GetBlock(std::vector<BlockPtr>& blocks) { return 0; }
    virtual int InsertBlock(const KVChain& kv_chain) { return 0; }
    virtual int Remove(BLOCK_KEY key) { return 0; }
    virtual int Clear() { return 0; }

    virtual int64_t GetCount() { return valid_ ? std::atomic_load<int64_t>(&count) : 0; }
    virtual void IncCount(int x) { std::atomic_fetch_add<int64_t>(&count, count+x); }
    virtual void ZeroCount() { std::atomic_store<int64_t>(&count, 0); }

    virtual void Acquire() { std::atomic_fetch_add<int64_t>(&used_, 1); }
    virtual void Release() { std::atomic_fetch_sub<int64_t>(&used_, 1); }
    virtual bool Freedom() { return std::atomic_load<int64_t>(&used_) == 0; }
    virtual void Wait() {
        int64_t t = 0;
        std::chrono::milliseconds ms(1);
        while (!std::atomic_compare_exchange_strong<int64_t>(&used_, &t, 0)) {
            std::this_thread::sleep_for(ms);
        }
    }

private:
	BlockPtr value_;
    BLOCK_TYPE type_;
    std::atomic<int64_t> count;
    std::atomic<int64_t> used_;
    int ttl;
    BLOCK_KEY key_;

protected:
    std::atomic<bool> valid_;
    std::mutex rwlock_;
};


/*
 * acquire helper
 */
class AcquireHelper {
public:
    AcquireHelper(BlockInterface* block): block_(block), released(false) {
        block_->Acquire();
    }
    virtual ~AcquireHelper() { if (!released) block_->Release(); }
    void Release() { released = true; block_->Release(); }
private:
    BlockInterface* block_;
    bool released;
};


/*
 * BaseBlock, used for normal memory
 */
class BaseBlock : public BlockInterface {
public:
    BaseBlock(): BlockInterface(BASE_BLOCK) {}
    BaseBlock(BLOCK_KEY key): BlockInterface(key, BASE_BLOCK) {}
    virtual ~BaseBlock() {}

    virtual void SetInvalid() override { Marking(false); }
    virtual void SetValid() override { Marking(true); }

    virtual int Remove(BLOCK_KEY key) override {
        std::lock_guard<std::mutex> rw(rwlock_);

        if (sub_key_.find(key) == sub_key_.end()) return -1;
        sub_key_[key]->SetInvalid();
        if (!sub_key_[key]->Freedom()) sub_key_[key]->Wait();

        IncCount(-sub_key_[key]->GetCount());
        sub_key_.erase(key);
        return 0;
    }

    virtual int Clear() override {
        if (Valid()) SetInvalid();
        if (!Freedom()) Wait();
        for (auto & k : sub_key_) {
            k.second->Clear();
        }
        sub_key_.clear();
        ZeroCount();
        return 0;
    }

	virtual BlockPtr Get(BLOCK_KEY key) override {
        //std::lock_guard<std::mutex> rw(rwlock_);
		AcquireHelper aq(this);
		if (Valid() && sub_key_.find(key) != sub_key_.end()) {
			return sub_key_[key];
		}
    	return NULL;
    }

    /*
     * TODO: replace with BFS
     */
    virtual int GetBlock(std::vector<BlockPtr>& blocks) override {
        auto tmp = sub_key_;
        {
            //std::lock_guard<std::mutex> rw(rwlock_);
            AcquireHelper aq(this);
            if (!Valid()) return false;
            if (!IsBaseBlock()) {
                blocks.push_back(GetValue());
                return 0;
            }
        }
        for (auto & k : tmp) {
            k.second->GetBlock(blocks);
        }
	    return 0;
    }

    /*
     * TODO: add count
     */
    virtual int InsertBlock(const KVChain& kv_chain) override {
        int newsub = 0;
        BlockPtr subkey;
        {
            std::lock_guard<std::mutex> rw(rwlock_);
            AcquireHelper aq(this);
            if (!Valid()) return false;
            if (kv_chain.key.empty()) {
                if (!sub_key_.empty()) return 0;
                SetValue(kv_chain.value);
                SetType(UDEF_VALUE);
                return 0;
            }
            if (sub_key_.find(kv_chain.key[0]) == sub_key_.end()) {
                sub_key_[kv_chain.key[0]] = std::make_shared<BaseBlock>(kv_chain.key[0]);
                IncCount(1);
                newsub = 1;
            }
            subkey = sub_key_[kv_chain.key[0]];
        }
        int ret = subkey->InsertBlock({
            kv_chain.level-1,
            {kv_chain.key.begin()+1, kv_chain.key.end()},
            kv_chain.value});

        if (!newsub && ret) IncCount(1);

        return ret | newsub;
    }

private:
    void Marking(bool valid) {
        std::atomic_store(&valid_, valid);
        for (auto & k : sub_key_) {
            valid ? k.second->SetValid() : k.second->SetInvalid();
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
