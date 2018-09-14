//
// Created by hhx on 2018/9/14.
//

#include "rwlock.h"
#include <unistd.h>
#include <thread>

MCKVS::RWLock lk;
int k = 9527;
char v[256];

void rworker(int x)
{
    int i = 0;
    int j;
    while (i++ < 1000000) {
        sleep(2);
        MCKVS::RWLockGuard<MCKVS::RWLock> lock_(lk, false);
        sprintf(v, "read k %d\n", k);
        write(1, v, strlen(v));
    }
}

void wworker(int x)
{
    int i = 0;
    while (i++ < 1000000) {
        sleep(3);
        MCKVS::RWLockGuard<MCKVS::RWLock> lock_(lk, true);
        k += 3;
        sprintf(v, "writing k %d\n", k);
        write(1, v, strlen(v));
    }
}

void TestRWLock()
{
    std::thread rt[10], wt[10];

    for (int i = 0; i < 10; ++i) {
        rt[i] = std::thread(rworker, i);
    }

    sleep(1);

    for (int i = 0; i < 8; ++i) {
        wt[i] = std::thread(wworker, i);
    }

    for (int i = 0; i < 8; ++i) {
        wt[i].join();
    }
    for (int i = 0; i < 10; ++i) {
        rt[i].join();
    }
}
