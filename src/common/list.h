//
// Created by hhx on 2018/9/17.
//

#ifndef TBKVS_LIST_H
#define TBKVS_LIST_H

#include <mutex>
#include <queue>
#include <condition_variable>

namespace MCKVS
{

#define LIST_MAX_ITEMS  1000000

template <typename T_>
class LFList {
public:
    LFList(): LFList(LIST_MAX_ITEMS) { }
    LFList(int max_): max_items_(max_) { }
    virtual ~LFList() { }

    bool Push(const T_ &t) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (q_.size() == max_items_) {
                return false;
            }
            q_.push(std::move(t));
        }
        push_cv_.notify_one();
        return true;
    }

    bool Pop(T_ &value) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            push_cv_.wait(lock, [=]()-> bool{ return q_.size() > 0; });
            value = std::move(q_.front());
            q_.pop();
        }
        return true;
    }


private:
    size_t max_items_;
    std::mutex queue_mutex_;
    std::condition_variable push_cv_;

    std::queue<T_> q_;
};

}


#endif //TBKVS_LIST_H
