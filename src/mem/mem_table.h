//
// Created by hhx on 2018/9/14.
//

#ifndef TBKVS_MEM_TABLE_H
#define TBKVS_MEM_TABLE_H

#include <vector>
#include "base_block.h"
#include "common/rwlock.h"
#include "common/convert.h"

namespace MCKVS
{

class MemTable {
public:
    MemTable() { root_ = std::make_shared<BaseBlock>(); }
    virtual ~MemTable() {}

    bool Get(const std::string& key, std::vector<BlockPtr>& value) {
        auto vec = UTIL::split(&key[1]);
        auto p = root_;
        for (auto k : vec) {
            if (p == NULL) return false;
            p = p->Get(k);
        }
        if (p && p->Valid()) {
            if (!p->IsBaseBlock()) value.push_back(p);
            else p->GetLeafBlock(value);
            return true;
        }
        return false;
    }

    bool Set(const std::string& key, const BlockPtr& value) {
        KVChain kv_chain = {0};
        for (auto k : UTIL::split(&key[1])) {
            kv_chain.key.push_back(k);
            kv_chain.level++;
        }
        kv_chain.value = value;
        return root_->InsertBlock(kv_chain);
    }

    bool Del(const std::string& key) {
        auto p = root_, q = root_;
        BLOCK_KEY subkey;
        for (auto k : UTIL::split(&key[1])) {
            if (q == NULL) return false;
            p = q;
            q = q->Get(k);
            subkey = k;
        }
        if (q->IsBaseBlock()) return q->Clear();
        else return p->Remove(subkey);
    }

private:
    BlockPtr root_;
};


}

#endif //TBKVS_MEM_TABLE_H
