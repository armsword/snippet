#include <iostream>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>
#include <fstream>

struct arg_struct {  
    const std::vector<int> *first;
    uint32_t first_size_begin;
    uint32_t first_size_end;
    const std::vector<int> *second;
    uint32_t second_size_begin;
    uint32_t second_size_end;
    std::vector<int> value;
};

typedef struct arg_struct ARG ;

void* merge(void *arg) {    
    ARG *p = (ARG *) arg;
    
    int first_size = p->first_size_end;
    int second_size = p->second_size_end;
    int begin = p->first_size_begin;
    int end = p->second_size_begin;
 
    //std::cout << "first begin size: " << i << " first end size: " << first_size << " second begin size: " << j << " second end size: " << second_size << std::endl;
    while (begin < first_size && end < second_size) {
        if ((*p->first)[begin] < (*p->second)[end]) {
            p->value.push_back((*p->first)[begin]);
            begin++;
        } else if ((*p->first)[begin] > (*p->second)[end]){
            p->value.push_back((*p->second)[end]);
            end++;
        } else {
            p->value.push_back((*p->second)[begin]);
            begin++;
            end++;
        }
    }

    while (begin < first_size) {
        p->value.push_back((*p->first)[begin]);
        begin++;
    }

    while (end < second_size) {
        p->value.push_back((*p->second)[end]);
        end++;
    }
    
    std::cout << "value size is " << p->value.size() << std::endl;
    return NULL;
}

void create_n_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count)
{
    pthread_t thr[thread_count];

    ARG arg[thread_count];
    int pos = 0;
    for (size_t i = 0; i < thread_count; i++) {
        std::cout << "pos is " << pos << " value is " << second[pos] << std::endl;
        arg[i].first = &first;
        arg[i].first_size_begin = first.size() * i / thread_count;
        arg[i].first_size_end = first.size() * (i + 1) / thread_count;
        arg[i].second = &second;
        arg[i].second_size_begin = pos;
        if (i == thread_count - 1) {
            arg[i].second_size_end = second.size();
        } else {
            pos = lower_bound(second.begin(), second.end(), first[first.size() * (i + 1) / thread_count]) - second.begin();
            arg[i].second_size_end = pos; // + 1 
        }
    }
    
    for (size_t i = 0; i < thread_count; i++) {
        if (pthread_create(&thr[i], NULL, merge, (void*)&arg[i]) !=0) {
            std::cout << "create " << i << " thread failed!" << std::endl;
            return;
        }
    }
    
    for (size_t i = 0; i < thread_count; i++) {
        pthread_join(thr[i], NULL);
    }
}

void load_data(std::vector<int> &first, std::vector<int> &second) {
    std::ifstream fin("first.txt", std::ios::in);
    char line[256]={0};
    while (fin.getline(line, sizeof(line))) {
        first.push_back(atoi(line));
    }
   
    std::ifstream fin2("second.txt", std::ios::in);
    while (fin2.getline(line, sizeof(line))) {
        second.push_back(atoi(line));
    }
}

void common(const std::vector<int> &first, const std::vector<int> &second) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);    
    ARG arg;
    arg.first = &first;
    arg.first_size_begin = 0;
    arg.first_size_end = first.size();
    arg.second = &second;
    arg.second_size_begin = 0;
    arg.second_size_end = second.size();
    merge((void*)&arg);
    gettimeofday(&t2, NULL);
    std::cout << "一个进程merge耗时 " << ((t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec) <<" 微秒"<< std::endl; //微秒
}

void use_four_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);    
    create_n_thread(first, second, thread_count);
    gettimeofday(&t2, NULL);
    std::cout << "四个线程merge耗时 " << ((t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec) <<" 微秒"<< std::endl; //微秒
}

void use_two_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);    
    create_n_thread(first, second, thread_count);
    gettimeofday(&t2, NULL);
    std::cout << "两个线程merge耗时 " << ((t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec) <<" 微秒"<< std::endl; //微秒
}


int main() {    
    std::vector<int> first;
    std::vector<int> second;
    load_data(first, second);

    // 单线程merge时间统计
    common(first, second);
    
    //2个线程merge时间统计
    use_two_thread(first, second, 2);

    //4个线程merge时间统计
    use_four_thread(first, second, 4);
    return 0;
}
