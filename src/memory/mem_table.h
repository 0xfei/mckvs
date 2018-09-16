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

    /*
     * Key: /ROOT/GROUP1/GROUP2, split key into a vector
     *      must start with '/'
     */
    int Get(const std::string& key, std::vector<BlockPtr>& value) {
        return Get(UTIL::split(&key[1]), value);
    }

    int Set(const std::string& key, const BlockPtr& value) {
        return Set(UTIL::split(&key[1]), value);
    }

    int Del(const std::string& key) {
        return Del(UTIL::split(&key[1]));
    }

    /*
     * Get/Set/Del impl
     */
    int Get(const std::vector<std::string>& vec, std::vector<BlockPtr>& value) {
        auto p = root_;
        for (auto k : vec) {
            p = p->Get(k);
	        if (p == NULL) return false;
        }
	    return p->GetBlock(value);
    }

    int Set(const std::vector<std::string>& vec, const BlockPtr& value) {
        KVChain kv_chain = {0};
        for (auto k : vec) {
            kv_chain.key.push_back(k);
            kv_chain.level++;
        }
        kv_chain.value = value;
        return root_->InsertBlock(kv_chain);
    }

    int Del(const std::vector<std::string>& vec) {
        auto p = root_, q = root_;
        BLOCK_KEY subkey;
        for (auto k : vec) {
            p = q;
            q = q->Get(k);
            subkey = k;
	        if (q == NULL) return false;
        }
        p->Remove(subkey);
        //TODO: maybe useless
        //if (q->IsBaseBlock()) return q->Clear();
        return 0;
    }

private:
    BlockPtr root_;
};


}

#endif //TBKVS_MEM_TABLE_H
