/*ENSC 351 Lab 4
Authors: Bennett Sasaki & Jordan Kam

*/
#include "Header.h"

using namespace std;

constexpr int MAX_THREADS = 4;
ofstream out_file;
std::vector<myMap> maps;
std::vector<myMap> reduced_pairs;
double single_time, map_time = 0;


int main() {
	ifstream in_file;
	out_file.open("mapReduceOut.txt");
	in_file.open("L4In.txt", std::ifstream::in);
	out_file << "---------------MapReduce---------------\n";
	std::vector<std::string> words; //Vector to hold unsorted words
	words = input_read(in_file);

#pragma omp parallel for
	for (int i = 0; i < words.size(); i++)
		map_thread(words[i]);

	bubblesort(maps); //maps is now sorted alphabetically
	std::vector<std::vector<myMap>> groups_of_keys = pre_reduce(maps); //groups_of_keys now holds vectors that each hold pairs with the same keys

#pragma omp parallel for
	for (int i = 0; i < groups_of_keys.size(); i++)
		reduce_thread(groups_of_keys[i]);

	sort_by_value(reduced_pairs);

	for (int i = 0; i < reduced_pairs.size(); i++)
		output(reduced_pairs[i]);

	in_file.close();
	out_file.close();
	return 0;
}

void map_thread(std::string key) {
/* map_thread- section 4.2
   Purpose: creates key-value pair
   Input: a single key
   Output: key-value pair with a value of 1
*/
	myMap m;
#pragma omp critical
	{
	m = create_pair(key);
	maps.push_back(m);
	}
}

void reduce_thread(std::vector<myMap> pairs) {
/* reduce_thread- section 4.2
	Purpose: creates a single key-value pair and pushes into reduced_pairs
	Input: vector with pairs, all with the same keys
	Output: N/A
*/
	myMap m;
#pragma omp critical 
	{
	m = reduce(pairs);
	reduced_pairs.push_back(m);
	}
}

std::vector<std::string> input_read(std::ifstream& in_file) {
/*  input_read- Section 4.1.1
	Purpose: return vector where each memeber is a word
	Input: text file to perform word count on
	Output: vector with each word separated
	Note: does not check for delimeters, only spaces.
			We can fix that later if we need to
*/
	std::vector<std::string> vect;
	char* word = new char;
	while (in_file >> word) {
		vect.push_back(word);
	}
	return vect;
}

myMap create_pair(std::string key) {
/*  create_pair- Section 4.1.2
	Formerly called "map_func"
	Purpose: creates a key-value pair with a value one for a given key
	Input: string which will be the key of the key-value pair
	Output: key-value pair
*/
	myMap m;
	m.key = key;
	m.value = 1;
	return m;
}

myMap reduce(std::vector<myMap> v) {
/*  reduce- Section 4.1.3
	Purpose: creates a single key-value pair given a list of pairs with the same keys
	Input: list of key-value pairs, which should have the same key
	Output: a single key-value pair with correct value
*/
	myMap m;
	m.key = v[0].key;
	m.value = v.size();
	return m;
}

void output(myMap m) {
/*  create_pair- Section 4.1.4
	Purpose: outputs word count for a given key-value pair
	Input: a single key-value pair
	Output: N/A
*/
	out_file << m.key << ": " << m.value << "\n";
	return;
}

void bubblesort(std::vector<myMap> &strings) {
	typedef std::vector<myMap>::size_type size_type;
	for (size_type i = 1; i < strings.size(); ++i)
	{
		for (size_type j = 0; j < (strings.size() - 1); ++j)
		{
			if (strings[j].key > strings[j + 1].key)
			{
				myMap temp = strings[j];
				strings[j].key = strings[j + 1].key;
				strings[j + 1] = temp;
			}
		}
	}
}

void sort_by_value(std::vector<myMap> &strings) {
	typedef std::vector<myMap>::size_type size_type;
	for (size_type i = 1; i < strings.size(); ++i)
	{
		for (size_type j = 0; j < (strings.size() - 1); ++j)
		{
			if (strings[j].value < strings[j + 1].value)
			{
				myMap temp = strings[j];
				strings[j] = strings[j + 1];
				strings[j + 1] = temp;
			}
		}
	}
}

std::vector<std::vector<myMap>> pre_reduce(std::vector<myMap> single_words) {
/*  pre-reduce
	this function takes the bubble-sorted pairs and groups together the pairs with the same keys, so they can go into reduce
	Input: Vector of maps, already sorted alphabetically
	Output: Vector of vectors of maps, each vector holding all the pairs with the same key
*/
	std::vector<std::vector<myMap>> master;
	std::vector<myMap> dummy;
	dummy.push_back(single_words[0]);
	std::string current_key = single_words[0].key;
	for (int i = 1; i < single_words.size(); i++) {
		if (single_words[i].key == current_key) {
			dummy.push_back(single_words[i]);
			continue;
		}
		else {
			master.push_back(dummy);
			dummy.clear();
			current_key = single_words[i].key;
			dummy.push_back(single_words[i]);
		}
	}
	master.push_back(dummy);
	return master;
}