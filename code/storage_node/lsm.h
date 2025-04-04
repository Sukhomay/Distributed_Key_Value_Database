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

using namespace std;
namespace fs = std::filesystem;

enum class ReturnStatus {
    FAILURE = 0,
    SUCCESS = 1
};

const char DELIMITER = '#';
const string TOMBSTONE = "tombstone";
const int MAX_FILE_SIZE = 4096;
const int INDEX_SIZE = 512;
const int MAX_COMP_TIME = 100000;
const int MIN_COMP_TIME = 1;

// Max AVL Tree size in memory 
const int MAX_TREE_SIZE = 1000;

class Semaphore;
class AVLTree;
class SSTable;
class BloomFilter;