#include <fstream>
#include <set>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <stdint.h>

void prepare_data(std::vector<int> &first, std::vector<int> &second, uint32_t l1, uint32_t l2) {
    std::set<int> temp_first;
    std::set<int> temp_second;

    while (temp_first.size() < l1) {
        int i = rand()  % 500000000;
        std::set<int>::iterator it = temp_first.find(i);
        if (it == temp_first.end()) {
            temp_first.insert(i);
        }        
    }    
    for (std::set<int>::iterator it = temp_first.begin(); it != temp_first.end(); it++) {
        first.push_back(*it);
    }    
    sort(first.begin(), first.end());        
    
    while (temp_second.size() < l2) {
        int i = rand()  % 500000000;
        std::set<int>::iterator it = temp_second.find(i);
        if (it == temp_second.end()) {
            temp_second.insert(i);
        }        
    }

    for (std::set<int>::iterator it = temp_second.begin(); it != temp_second.end(); it++) {
        second.push_back(*it);
    }
    sort(second.begin(), second.end());    
}

int main(int argc, char *argv[]) {    
    if (argc < 3) {
        std::cout << "./prepare xxx xxx" << std::endl;
        return -1;
    }
    uint32_t l1 = atoi(argv[1]);
    uint32_t l2 = atoi(argv[2]);
    std::ofstream  ofile1;
    ofile1.open("first.txt");
    std::ofstream ofile2;
    ofile2.open("second.txt");
    std::vector<int> first;
    std::vector<int> second;
    
    srand((int)time(0));
    prepare_data(first, second, l1, l2);
    
    for (std::vector<int>::iterator it = first.begin(); it != first.end(); it++) {
        ofile1 << *it << std::endl;
    }

    for (std::vector<int>::iterator it = second.begin(); it != second.end(); it++) {
        ofile2 << *it << std::endl;
    }

    ofile1.close();
    ofile2.close();
    return 0;    
}
