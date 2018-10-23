/*ENSC 351 Lab 4
Authors: Bennett Sasaki & Jordan Kam

*/
#include "Header.h"

using namespace std;

constexpr int MAX_THREADS = 4;
mutex mtx;
ofstream out_file;
std::vector<myMap> maps;
std::vector<myMap> reduced_pairs;
std::chrono::time_point<std::chrono::high_resolution_clock, std::chrono::high_resolution_clock::duration> init_time;
double single_time, map_time = 0;


int main() {
	ifstream in_file;
	out_file.open("mapReduceOut_____.txt");
	in_file.open("L4In.txt", std::ifstream::in);
	out_file << "\n---------------MapReduce---------------\n";
	std::vector<std::string> words; //Vector to hold unsorted words
	words = input_read(in_file);

#pragma omp parallel for
	for (int i = 0; i < words.size(); i++)
	{
		map_thread(words, i);
	}

	bubblesort(maps); //maps is now sorted alphabetically
	std::vector<std::vector<myMap>> groups_of_keys = pre_reduce(maps); //groups_of_keys now holds vectors that each hold pairs with the same keys

#pragma omp parallel for
	for (int i = 0; i < groups_of_keys.size(); i++) {
		reduce_thread(groups_of_keys, i);
	}

	sort_by_value(reduced_pairs);

	for (int i = 0; i < reduced_pairs.size(); i++) {
		output(reduced_pairs[i]);
	}

	in_file.close();
	return 0;
}

void map_thread(std::vector<std::string> words, int tid) {
	myMap m;
	mtx.lock();
	m = create_pair(words[tid]);
	maps.push_back(m);
	mtx.unlock();
}

void reduce_thread(std::vector<std::vector<myMap>> maps, int tid) {
	myMap m;
	mtx.lock();
	m = reduce(maps[tid]);
	reduced_pairs.push_back(m);
	mtx.unlock();
}

std::vector<myMap> first_word_counter(ifstream& in_file) {
	/* word_counter- Section 3
	   Purpose: return word count
	   Input: text file to perform word count on
	   Output: total word count
	   Note: does not check for delimeters, only spaces.
			 We can fix that later if we need to
	*/
	std::vector<myMap> pairs;
	myMap m;
	char* word = new char;
	while (in_file >> word) {
		int i = 0;
		while (true) {
			if (i == pairs.size()) {
				m.key = word;
				m.value = 1;
				pairs.push_back(m);
				break;
			}
			else if (word == pairs[i].key) {
				pairs[i].value++;
				break;
			}
			i++;
		}

	}

	return pairs;
}

std::vector<std::string> input_read(std::ifstream& in_file) {
	/* input_read- Section 4.1.1
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
	/* create_pair- Section 4.1.2
	   Formerly called "map_func"
	   Purpose: creates a key-value pair with a value one for a given key
	   Input: string which will be the key of the key-value pair
	   Output: key-value pair
	*/
	myMap m;
	m.key = key;
	m.value = 1;
	//out_file << m.key << " " << m.value << endl;
	return m;
}

myMap reduce(std::vector<myMap> v1) {
	/* create_pair- Section 4.1.3
	   Purpose: creates a single key-value pair given a list of pairs with the same keys
	   Input: list of key-value pairs, which should have the same key
	   Output: a single key-value pair with correct value
	*/

	myMap m1;
	m1.key = v1[0].key;
	m1.value = v1.size();
	return m1;
}

void output(myMap m) {
	/* create_pair- Section 4.1.4
   Purpose: outputs word count for a given key-value pair
   Input: a single key-value pair
   Output: N/A
*/
	out_file << m.key << ": " << m.value << "\n";
	return;
}

void bubblesort(std::vector<myMap> &strings) {
	typedef std::vector<myMap>::size_type size_type;
	for (size_type i = 1; i < strings.size(); ++i) // for n-1 passes
	{
		// In pass i,compare the first n-i elements
		// with their next elements
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
	for (size_type i = 1; i < strings.size(); ++i) // for n-1 passes
	{
		// In pass i,compare the first n-i elements
		// with their next elements
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
	/* pre-reduce
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