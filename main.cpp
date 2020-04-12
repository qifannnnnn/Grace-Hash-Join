#include <iostream>
#include <string>
#include <functional>
#include <fstream>

#include "Join.hpp"
#include "Record.hpp"
#include "Page.hpp"
#include "Disk.hpp"
#include "Mem.hpp"
#include "Bucket.hpp"
#include "constants.hpp"
#include <assert.h>
#include <algorithm>
using namespace std;

/* Correct answer */
size_t ans_num_left_record = 0;
size_t ans_num_right_record = 0;
size_t ans_num_load_from_disk = 0;
size_t ans_num_flush_to_disk = 0;


void print(vector<unsigned int> &join_res, Disk* disk) {
	cout << "Size of GHJ result: " << join_res.size() << " pages" << endl;
	for (unsigned int i = 0; i < join_res.size(); ++i) {
		Page *join_page = disk->diskRead(join_res[i]);
		cout << "Page " << i << " with disk id = " << join_res[i] << endl;
		join_page->print();
	}
}

void parse_solution_file(const char* file_name, vector<Record> &join_solution) {
	string str_file_name(file_name);
	ifstream sol_file_stream(str_file_name);
	string one_line;
	getline(sol_file_stream, one_line);
	ans_num_left_record = atoi(one_line.c_str());
	getline(sol_file_stream, one_line);
	ans_num_right_record = atoi(one_line.c_str());
	getline(sol_file_stream, one_line);
	ans_num_load_from_disk = atoi(one_line.c_str());
	getline(sol_file_stream, one_line);
	ans_num_flush_to_disk = atoi(one_line.c_str());
	/* Read all the join record pair */
	while (getline(sol_file_stream, one_line)) {
		size_t space_idx = one_line.find(' ');
		string key = one_line.substr(0, space_idx);
		string data = one_line.substr(space_idx + 1);
		join_solution.push_back(Record(key, data));
	}
	sol_file_stream.close();
	std::sort(join_solution.begin(), join_solution.end());
	return;
}


void verify (const char* left_file, const char* right_file) {
	/* Read all the raw data from txt file */
	string str_file_name(left_file);
	ifstream raw_data_file(str_file_name);
	string one_line;
	/* Create the first new disk page */
	vector<string> left_key;
	vector<string> right_key;
	while (getline(raw_data_file, one_line)) {
		size_t space_idx = one_line.find(' ');
		string key = one_line.substr(0, space_idx);
		left_key.push_back(key);
	}
	raw_data_file.close();
	string str_file_name_new(right_file);
	ifstream raw_data_file_new(str_file_name_new);
	while (getline(raw_data_file_new, one_line)) {
		size_t space_idx = one_line.find(' ');
		string key = one_line.substr(0, space_idx);
		right_key.push_back(key);
	}
	raw_data_file_new.close();
	int count = 0;
	for (size_t i = 0; i < left_key.size(); ++i) {
		for (size_t j = 0; j < right_key.size(); ++j) {
			if (left_key[i] == right_key[j]) {
				++count;
			}
		}
	}
	cout << "Join result should be: " << count << endl;
}



int main(int argc, char* argv[]) {
	/* Parse cmd arguments */
	if (argc != 4) {
		cerr << "Error: Wrong command line usage." << endl;
		cerr << "Usage: ./GHJ left_rel.txt right_rel.txt solution.txt" << endl;
		exit(1);
	}
	vector<Record> join_solution;
	/* Read solution file */
	parse_solution_file(argv[3], join_solution);

	/* Variable initialization */
	Disk disk = Disk();
	Mem mem = Mem();
	pair<unsigned int, unsigned int> left_rel = disk.read_data(argv[1]);
	pair<unsigned int, unsigned int> right_rel = disk.read_data(argv[2]);
	
	/* Grace Hash Join Partition Phase */
	vector<Bucket> res = partition(&disk, &mem, left_rel, right_rel);

	/* Check the number of all data records */
	size_t student_num_left_record = 0;
	size_t student_num_right_record = 0;
	for (unsigned int i = 0; i < res.size(); ++i) {
		student_num_left_record += res[i].num_left_rel_record;
		student_num_right_record += res[i].num_right_rel_record;
	}
	if (ans_num_left_record != student_num_left_record
	 || ans_num_right_record != student_num_right_record) {
		cout << "Error: number of records after partition is wrong." << endl;
		exit(1);		
	}
	/* check the hash value of each record inside one bucket */ 
	for (unsigned int i = 0; i < res.size(); ++i) {
		Bucket bucket = res[i];
		vector<unsigned int> left = bucket.get_left_rel();
		vector<unsigned int> right = bucket.get_right_rel();
		/* If the bucket is empty */
		if (left.size() == 0 && right.size() == 0) {
			continue;
		}
		unsigned int hash_val;
		if (left.size() > 0) {
			hash_val = disk.diskRead(left[0])->get_record(0).partition_hash() % (MEM_SIZE_IN_PAGE - 1);
		}
		else if (right.size() > 0) {
			hash_val = disk.diskRead(right[0])->get_record(0).partition_hash() % (MEM_SIZE_IN_PAGE - 1);
		}
		for (unsigned int i = 0; i < left.size(); ++i) {
			for (unsigned int j = 0; j < disk.diskRead(left[i])->size(); ++j) {
				unsigned int each_hash_val = disk.diskRead(left[i])->get_record(j).partition_hash() % (MEM_SIZE_IN_PAGE - 1);
				if (each_hash_val != hash_val) {
					cout << "Error: records with different hash value (h1) are in the same bucket." << endl;
					exit(1);
				}
			}
		}
		for (unsigned int i = 0; i < right.size(); ++i) {
			for (unsigned int j = 0; j < disk.diskRead(right[i])->size(); ++j) {
				unsigned int each_hash_val = disk.diskRead(right[i])->get_record(j).partition_hash() % (MEM_SIZE_IN_PAGE - 1);
				if (each_hash_val != hash_val) {
					cout << "Error: records with different hash value (h1) are in the same bucket." << endl;
					exit(1);
				}
			}
		}
	}

	/* Grace Hash Join Probe Phase */
	vector<unsigned int> join_res = probe(&disk, &mem, res);

	/* For grading */
	/* Check the times of loading from and flushing to disk */
	if (mem.loadFromDiskTimes() != ans_num_load_from_disk) {
		cout << "Error: number of loading from disk is wrong." << endl;
		exit(1);
	}
	if (mem.flushToDiskTimes() != ans_num_flush_to_disk) {
		cout << "Error: number of flushing to disk is wrong." << endl;
		exit(1);
	}

	/* Check the size and each record of output after sorting */
	vector<Record> student_join;
	for (unsigned int i = 0; i < join_res.size(); ++i) {
		for (unsigned int j = 0; j < disk.diskRead(join_res[i])->size(); ++j) {
			student_join.push_back(disk.diskRead(join_res[i])->get_record(j));
		}
	}
	std::sort(student_join.begin(), student_join.end());
	if (join_solution.size() != student_join.size()) {
		cout << "Error: size of output is wrong." << endl;
		exit(1);
	}
	else {
		for (unsigned int i = 0; i < join_solution.size(); ++i) {
			if (!join_solution[i].equal(student_join[i])) {
				cout << "Error: wrong join output." << endl;
				exit(1);
			}
		}
	}

	cout << "Correct" << endl;
	return 88;
}
