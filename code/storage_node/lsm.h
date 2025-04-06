#include <iostream>
#include <bitset>
#include <cmath>
#include <functional>
#include <string>
#include <vector>
#include <thread>
#include <semaphore>
#include <chrono>
#include <filesystem>
#include <unistd.h>

#include "../db.h"

using namespace std;
namespace fs = std::filesystem;


const char DELIMITER = '#';
const string TOMBSTONE = "TOMBSTONE";
const int MAX_FILE_SIZE = 16;
const int INDEX_SIZE = 4;  // Maximum pairs per file
const int MAX_COMP_TIME = 10;
const int MIN_COMP_TIME = 1;

// Max AVL Tree size in memory 
const int MAX_TREE_SIZE = 1000;

class SSTable;
class MergeTree;