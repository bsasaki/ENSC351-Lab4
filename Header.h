#pragma once

#include <iostream>
#include <fstream>
#include <istream>
#include <ostream>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <omp.h>

struct myMap {
	std::string key;
	int value;
};

void map_thread(std::vector<std::string>, int tid);
void reduce_thread(std::vector<std::vector<myMap>> maps, int tid);
std::vector<myMap> first_word_counter(std::ifstream& in_file);
std::vector<std::string> input_read(std::ifstream& in_file);
myMap create_pair(std::string key);
myMap reduce(std::vector<myMap> v1);
void output(myMap m);
void bubblesort(std::vector<myMap> &strings);
void sort_by_value(std::vector<myMap> &strings);
std::vector<std::vector<myMap>> pre_reduce(std::vector<myMap> single_words);