//
// Created by hhx on 2018/9/17.
//

#include "binlog.h"
#include <sys/stat.h>
#include <unistd.h>


namespace MCKVS
{


/*
 * write log-struct into file
 */
void WriteAheadThread(BinlogManager *manager)
{
    while (!manager->exit_) {
        BinlogStruct stc;
        if (manager->list_.Pop(stc)) {
            manager->WriteAheadLog(stc, false);
        }
    }
}

void MergeThread(BinlogManager *manager, int fn)
{
    int sdb = manager->file_[fn][BI_SDB];
    int bin = manager->file_[fn][BI_TMP];

    struct stat log_info, sdb_info;
    if (fstat(bin, &log_info) != 0 || fstat(sdb, &sdb_info) != 0) {
        return;
    }

    void* log_map = mmap(NULL, log_info.st_size, PROT_READ, 0, bin, 0);
    void* sdb_map = mmap(NULL, sdb_info.st_size, PROT_READ, 0, bin, 0);
    if (!log_map || !sdb_map) {
        return;
    }

    munmap(log_map, log_info.st_size);
    munmap(sdb_map, sdb_info.st_size);
    ftruncate(bin, 0);
}


/*
 * TODO: use common file interface
 * append to file
 */
bool BinlogManager::WriteAheadLog(const BinlogStruct &stc, bool lock)
{
    auto fn = stc.hash % config_.file_num;
    if (lock) {
        std::lock_guard<std::mutex> lk(file_mutex_[fn]);
        return WriteAheadLogImpl(fn, (void*)&stc, sizeof(stc)+stc.ksize+stc.vsize);
    }
    return WriteAheadLogImpl(fn, (void*)&stc, sizeof(stc)+stc.ksize+stc.vsize);
}


bool BinlogManager::WriteAheadLogImpl(int fn, void* buf, size_t n)
{
    if (write(file_[fn][BI_CUR], buf, n) != n) {
        return false;
    }
    file_size_[fn] += n;
    if (file_size_[fn] > BINLOG_FSIZE_LIMIT && !file_merge_[fn]) {
        // TODO: merge file, operate on a thread
        auto fd = file_[fn][BI_TMP];
        ftruncate(fd, 0);
        file_[fn][BI_TMP] = file_[fn][BI_CUR];
        file_[fn][BI_CUR] = fd;

        file_thread_[fn] = std::thread(MCKVS::MergeThread, this, fn);
    }

    return true;
}


/*
 * append log-struct into list
 */
bool BinlogManager::AppendToBinlog(const BinlogStruct &stc)
{
    if (config_.type == FREESYNC) {
        list_.Push(stc);
    } else {
        return WriteAheadLog(stc, true);
    }
    return true;
}

bool BinlogManager::RecoveryFromBinlog(std::vector<BinlogStruct> &vec)
{


    return true;
}


}
