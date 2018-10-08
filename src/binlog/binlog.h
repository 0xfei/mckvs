//
// Created by hhx on 2018/9/17.
//

#ifndef TBKVS_BINLOG_H
#define TBKVS_BINLOG_H

#include <atomic>
#include <string>
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include "common/list.h"

namespace MCKVS
{

#define SDB_BINLOG_PREFIX       "binlog_sdb_"
#define CUR_BINLOG_PREFIX       "binlog_cr1_"
#define TMP_BINLOG_PREFIX       "binlog_cr2_"
#define INF_BINLOG_PREFIX       "binlog_inf_"

#define BINLOG_FULLNAME(prefix_, num_) (prefix_#num_)

#define BINLOG_SDB_FILE(num_)   BINLOG_FULLNAME(SDB_BINLOG_PREFIX, num_)
#define BINLOG_CUR_FILE(num_)   BINLOG_FULLNAME(CUR_BINLOG_PREFIX, num_)
#define BINLOG_TMP_FILE(num_)   BINLOG_FULLNAME(TMP_BINLOG_PREFIX, num_)
#define BINLOG_INF_FILE(num_)   BINLOG_FULLNAME(INF_BINLOG_PREFIX, num_)

#define BINLOG_FSIZE_LIMIT      (12*1024*1024)  // 12M

#define BINLOG_NUM_MAX          100
#define BINLOG_NUM_DFT          10


struct BinlogStruct {
    int opt;
    int ttl;
    int hash;
    int vtype;
    int ksize;
    int vsize;
    uint8_t data[];
};


enum BCONFIG {
    FREESYNC = 0,
    REALSYNC = 1,
};

struct BinlogConfig {
    BCONFIG type;
    int file_num;       // sdb file number, max-100
};

struct BinlogState {
    int log_index;
    int sdb_index;
};

enum BINDEX {
    BI_CUR = 0,
    BI_TMP = 1,
    BI_SDB = 2,
    BI_SAT = 3,
    BI_NUM = 4,
};

#define OPEN_FILE(fname_, index_, tag_) { \
    auto fd = open(fname_, O_RDWR|O_CREAT); \
    if (fd == -1) { \
        throw std::string("cannot open file: ") + fname_; \
    } \
    file_[index_][tag_] = fd; \
    }


class BinlogManager;
void WriteAheadThread(BinlogManager *manager);
void MergeThread(BinlogManager *manager, int fn);

class BinlogManager {
public:
    BinlogManager() : BinlogManager({FREESYNC, BINLOG_NUM_DFT}) {}
    BinlogManager(const BinlogConfig &conf) : config_(conf), exit_(false) {
        for (int i = 0; i < conf.file_num; ++i) {
            OPEN_FILE(BINLOG_CUR_FILE(i), i, BI_CUR);
            OPEN_FILE(BINLOG_TMP_FILE(i), i, BI_TMP);
            OPEN_FILE(BINLOG_SDB_FILE(i), i, BI_SDB);
            file_merge_[i] = false;
        }
        t_ = std::thread(WriteAheadThread, this);
    }
    virtual ~BinlogManager() {
        // TODO: manage merge thread, maybe ignore error is better
        exit_ = true;
        t_.join();
    }

    bool AppendToBinlog(const BinlogStruct &stc);
    bool RecoveryFromBinlog(std::vector<BinlogStruct> &vec);

private:
    bool WriteAheadLog(const BinlogStruct &stc, bool lock);
    bool WriteAheadLogImpl(int fn, void* buf, size_t n);

    bool exit_;

    BinlogConfig config_;
    std::thread t_;

    LFList<BinlogStruct> list_;

    int file_[BINLOG_NUM_MAX][BI_NUM];
    int file_size_[BINLOG_NUM_MAX];
    std::mutex file_mutex_[BINLOG_NUM_MAX];
    bool file_merge_[BINLOG_NUM_MAX];
    std::thread file_thread_[BINLOG_NUM_MAX];

    // friend
    friend void WriteAheadThread(BinlogManager *manager);
    friend void MergeThread(BinlogManager *manager, int fn);
};

}

#endif //TBKVS_BINLOG_H
