//
// Created by hhx on 2018/9/27.
//

#ifndef TBKVS_SERVER_H
#define TBKVS_SERVER_H


#include <thread>

struct WorkerThread {
    std::thread t;
    std::thread::id tid;
    int key;
};

std::thread t;

class Server {
public:
    Server() {}
    virtual ~Server() {}

private:

};

#endif //TBKVS_SERVER_H
