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
    p->value.resize((first_size - begin) + (second_size - end));

    int i = 0;
    while (begin < first_size && end < second_size) {
        if ((*p->first)[begin] < (*p->second)[end]) {
            p->value[i] = (*p->first)[begin];
            begin++;
        } else if ((*p->first)[begin] > (*p->second)[end]){
            p->value[i] = (*p->second)[end];
            end++;
        } else {
            p->value[i] = (*p->second)[begin];
            begin++;
            end++;
        }
        i++;
    }

    while (begin < first_size) {
        p->value[i] = (*p->first)[begin];
        begin++;
        i++;
    }

    while (end < second_size) {
        p->value[i] = (*p->second)[end];
        end++;
        i++;
    }
    p->value.resize(i);
    return NULL;
}

// N 是一个数，根据不同的数据特点选择不同的值，以解决数据倾斜问题
// 假如2个线程merge，那么不能完全保证2个线程里的数据长度相同，此时根据数据特点选择合适的N值即可
static int N = 1;
void find_middle_data_pos(const std::vector<int> &first, const std::vector<int> &second, 
                          uint32_t &first_pos, uint32_t &second_pos) {
    int first_begin = 0;
    int first_end = first.size();
    first_pos = first.size() / 2;
    int second_begin = 0;
    int second_end = second.size();
    second_pos = lower_bound(second.begin(), second.end(), first[first_pos]) - second.begin() + 1;
    while ((first_pos + second_pos) < (first.size() + second.size()) / 2 - N ||
           (first_pos + second_pos) > (first.size() + second.size()) / 2 + N) 
    {
        if ((first_pos + second_pos) < (first.size() + second.size()) / 2 - N) {
            first_begin = first_pos;
            first_pos = (first_begin + first_end) / 2;
            second_begin = second_pos;
            second_pos = lower_bound(second.begin(), second.end(), first[first_pos]) - second.begin() + 1;
        } else {
            first_end = first_pos;
            first_pos = (first_begin + first_end) / 2;
            second_end = second_pos;
            second_pos = lower_bound(second.begin(), second.end(), first[first_pos]) - second.begin() + 1;
        }
    }
}

void create_two_thread_4_dataskew(const std::vector<int> &first, const std::vector<int> &second)
{
    pthread_t thr[2];

    ARG arg[2];
    
    uint32_t first_pos = 0;
    uint32_t second_pos = 0;
    find_middle_data_pos(first, second, first_pos, second_pos);
    arg[1].first = &first;
    arg[1].first_size_begin = 0;
    arg[1].first_size_end = first_pos;
    arg[1].second = &second;
    arg[1].second_size_begin = 0;
    arg[1].second_size_end = second_pos;
    arg[2].first = &first;
    arg[2].first_size_begin = first_pos;
    arg[2].first_size_end = first.size();
    arg[2].second = &second;
    arg[2].second_size_begin = second_pos;
    arg[2].second_size_end = second.size();
    
    for (size_t i = 0; i < 2; i++) {
        if (pthread_create(&thr[i], NULL, merge, (void*)&arg[i]) !=0) {
            std::cout << "create " << i << " thread failed!" << std::endl;
            return;
        }
    }
    
    for (size_t i = 0; i < 2; i++) {
        pthread_join(thr[i], NULL);
    }
}

void create_n_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count)
{
    pthread_t thr[thread_count];

    ARG arg[thread_count];
    int pos = 0;
    for (size_t i = 0; i < thread_count; i++) {
        arg[i].first = &first;
        arg[i].first_size_begin = first.size() * i / thread_count;
        arg[i].first_size_end = first.size() * (i + 1) / thread_count;
        arg[i].second = &second;
        arg[i].second_size_begin = pos;
        if (i == thread_count - 1) {
            arg[i].second_size_end = second.size();
        } else {
            pos = lower_bound(second.begin(), second.end(), first[first.size() * (i + 1) / thread_count]) - second.begin();
            arg[i].second_size_end = pos; 
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

void use_8_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);    
    create_n_thread(first, second, thread_count);
    gettimeofday(&t2, NULL);
    std::cout << "八个线程merge耗时 " << ((t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec) <<" 微秒"<< std::endl; //微秒
}

void use_16_thread(const std::vector<int> &first, const std::vector<int> &second, uint32_t thread_count) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);    
    create_n_thread(first, second, thread_count);
    gettimeofday(&t2, NULL);
    std::cout << "十六个线程merge耗时 " << ((t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec) <<" 微秒"<< std::endl; //微秒
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
    
    // 2个线程merge时间统计
    use_two_thread(first, second, 2);

    // 4个线程merge时间统计
    use_four_thread(first, second, 4);
    
    use_8_thread(first, second, 8);

    use_16_thread(first, second, 16);

    return 0;
}
