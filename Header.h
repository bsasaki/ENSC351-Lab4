#pragma once

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <omp.h>

struct myMap {
	std::string key;
	int value;
};

void map_thread(std::string key);
void reduce_thread(std::vector<myMap> keys);
std::vector<std::string> input_read(std::ifstream& in_file);
myMap create_pair(std::string key);
myMap reduce(std::vector<myMap> v);
void output(myMap m);
void bubblesort(std::vector<myMap> &strings);
void sort_by_value(std::vector<myMap> &strings);
std::vector<std::vector<myMap>> pre_reduce(std::vector<myMap> single_words);