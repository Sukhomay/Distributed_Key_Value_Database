#include "lsm.h"
#include "avl_tree.cpp"
#include "bloomfilter.cpp"

#include <thread>
#include <mutex>

int comp_time = MAX_COMP_TIME;
mutex mtx_sstablelist;
AVLTree tree;
class SSTable;
vector<SSTable *> SSTable_list;

string encodeKeyValuePair(const string &key_str, const string &value_str)
{
    string result = key_str + DELIMITER + value_str + DELIMITER;
    return result;
}

pair<string, string> decodeKeyValuePair(const string &combinedStr)
{

    size_t pos = combinedStr.find(DELIMITER);
    string key_str, value_str;

    if (pos != string::npos)
    {
        key_str = combinedStr.substr(0, pos);
        value_str = combinedStr.substr(pos + 1);
    }
    value_str.pop_back();
    return {key_str, value_str};
}

// Function to split the string based on the delimiter
vector<string> splitString(const string &str)
{
    vector<string> tokens;
    string token;
    for (char ch : str)
    {
        if (ch == DELIMITER)
        {
            if (!token.empty())
            {
                tokens.push_back(token);
                token.clear();
            }
        }
        else
        {
            token += ch;
        }
    }
    if (!token.empty())
    {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to extract the integer pair at a specific index within a binary file
pair<int32_t, int32_t> extractPair(const string &filename, int pair_idx)
{
    ifstream file(filename, ios::binary);
    if (!file)
    {
        cerr << "Failed to open file: " << filename << endl;
        return {-1, -1};
    }

    // Calculate the offset: each pair takes 8 bytes (2 integers of 4 bytes each)
    streampos offset = pair_idx * 8;
    file.seekg(offset, ios::beg);

    int32_t key, value;
    file.read(reinterpret_cast<char *>(&key), sizeof(int32_t));
    file.read(reinterpret_cast<char *>(&value), sizeof(int32_t));

    if (!file)
    {
        cerr << "Error reading from file: " << filename << endl;
        return {-1, -1}; // Return an invalid pair if reading fails
    }

    return {key, value};
}

string extractKeyValuePair(const string &filename, streampos position)
{
    ifstream inFile(filename, ios::binary);
    if (!inFile)
    {
        throw runtime_error("Cannot open file");
    }

    inFile.seekg(position);
    if (!inFile)
    {
        throw runtime_error("Invalid position in file");
    }

    string result;
    char ch;
    int delimiterCount = 0;

    while (inFile.get(ch))
    {
        result += ch;
        if (ch == DELIMITER)
        {
            delimiterCount++;
            if (delimiterCount == 2)
            {
                break;
            }
        }
    }

    if (delimiterCount < 2)
    {
        throw runtime_error("Second occurrence of DELIMITER not found");
    }

    return result;
}

void createFolder(const string &folder_name)
{
    try
    {
        // Check if the folder already exists
        if (fs::exists(folder_name))
        {
            // cout << "Folder '" << folder_name << "' already exists." << endl;
        }
        else
        {
            // Create the folder
            if (fs::create_directory(folder_name))
            {
                // cout << "Folder '" << folder_name << "' created successfully." << endl;
            }
            else
            {
                cerr << "Failed to create folder '" << folder_name << "'." << endl;
            }
        }
    }
    catch (const exception &e)
    {
        cerr << "Error: " << e.what() << endl;
    }
}

void deleteFolder(const string &folder_name)
{
    try
    {
        // Check if the folder exists
        if (!fs::exists(folder_name))
        {
            return;
        }

        // Remove the folder and its contents
        fs::remove_all(folder_name);

        // cout << "Folder deleted successfully: " << folder_name << endl;
    }
    catch (const fs::filesystem_error &e)
    {
        cerr << "Error deleting folder: " << e.what() << endl;
    }
}

class SSTable
{

private:
    string folder_name;
    BloomFilter bfilter;
    int num_keys = 0;

public:
    SSTable(const pair<int, pair<string, string> *> &data = {0, nullptr}, string fname = TOMBSTONE)
    {
        if (fname == TOMBSTONE)
        {
            int idx = SSTable_list.size();
            folder_name = "SSTable_" + to_string(idx);
        }
        else
        {
            folder_name = fname;
        }
        createFolder(folder_name);

        // Extract the size and the data pointer
        num_keys = data.first;
        pair<string, string> *keyval_array = data.second;

        // Dynamically allocate array for hashed data
        string *hashed_data = new string[num_keys];

        for (int i = 0; i < num_keys; ++i)
        {
            // Encode key-value pairs and update Bloom filter
            hashed_data[i] = encodeKeyValuePair(keyval_array[i].first, keyval_array[i].second);
            bfilter.insert(keyval_array[i].first);
        }

        // Store hashed data and retrieve indices
        int indices_size = 0;
        pair<int, int> *indices = store_keyval_data(hashed_data, num_keys);

        // Store indices
        store_keyval_index(indices, num_keys);

        // Clean up dynamically allocated arrays
        delete[] hashed_data;
        delete[] indices;
    }

    ~SSTable()
    {
        deleteFolder(folder_name);
    }

    int get_num_keys()
    {
        return num_keys;
    }

    string get_folder_name()
    {
        return folder_name;
    }

    pair<bool, string> find(const string key)
    {
        if (bfilter.exists(key) && num_keys > 0)
        {
            int lo = 0, hi = num_keys - 1;
            while (lo <= hi)
            {
                int mid = (lo + hi) / 2;
                int file_idx = mid / INDEX_SIZE;
                int pair_idx = mid % INDEX_SIZE;
                string filename = folder_name + "/" + to_string(file_idx) + ".bin";

                pair<int32_t, int32_t> result = extractPair(filename, pair_idx);
                if (result.first == -1 || result.second == -1)
                {
                    // Handle the case where reading failed
                    cerr << "Error: Could not read pair at index " << pair_idx << " in file " << filename << endl;
                    exit(1);
                }
                int key_value_file_idx = result.first, key_value_pos_idx = result.second;
                string key_value_filename = folder_name + "/" + to_string(key_value_file_idx) + ".txt";

                string key_value = extractKeyValuePair(key_value_filename, key_value_pos_idx);
                auto p = decodeKeyValuePair(key_value);
                string curr_key = p.first, curr_value = p.second;

                if (curr_key == key)
                {
                    return make_pair(true, curr_value);
                }
                else if (curr_key > key)
                {
                    hi = mid - 1;
                }
                else
                {
                    lo = mid + 1;
                }
            }
        }
        return make_pair(false, TOMBSTONE);
    }

    void store_keyval_index(const pair<int, int> *data, int num_pairs)
    {
        const size_t maxPairsPerFile = INDEX_SIZE; // Maximum pairs per file
        size_t fileIndex = 0;

        for (int i = 0; i < num_pairs; i += maxPairsPerFile)
        {
            string filename = folder_name + "/" + to_string(fileIndex++) + ".bin";

            ofstream outFile(filename, ios::binary);
            if (!outFile)
            {
                cerr << "Error opening file: " << filename << endl;
                return;
            }

            // Determine the number of pairs to write in this file
            size_t pairsToWrite = min(static_cast<size_t>(maxPairsPerFile), static_cast<size_t>(num_pairs - i));

            for (size_t j = 0; j < pairsToWrite; ++j)
            {
                outFile.write(reinterpret_cast<const char *>(&data[i + j].first), sizeof(int));
                outFile.write(reinterpret_cast<const char *>(&data[i + j].second), sizeof(int));
            }

            outFile.close();
        }
    }

    pair<int, int> *store_keyval_data(const string *data, int num_keys)
    {
        const size_t maxFileSize = MAX_FILE_SIZE; // 4KB
        pair<int, int> *fileOffsets = new pair<int, int>[num_keys];

        int fileIndex = 0;
        size_t currentFileSize = 0;
        ofstream outFile;
        string filename = folder_name + "/" + "0.txt";
        outFile.open(filename);

        int offsetIndex = 0;
        for (int i = 0; i < num_keys; ++i)
        {
            const string &str = data[i];
            size_t newSize = currentFileSize + str.size() + 1;

            // If adding the current string exceeds max file size, open a new file
            if (newSize > maxFileSize)
            {
                outFile.close();
                currentFileSize = 0;
                filename = folder_name + "/" + to_string(++fileIndex) + ".txt";
                outFile.open(filename); // TODO
            }

            // Record the file index and current offset
            fileOffsets[offsetIndex++] = {fileIndex, static_cast<int>(currentFileSize)};

            // Write the string to the current file
            outFile << str;

            // Update the current file size
            currentFileSize += str.size();
        }

        outFile.close();

        return fileOffsets;
    }
};

ReturnStatus create_SSTable(vector<pair<string, string>> &data)
{
    try
    {
        // Convert vector to dynamically allocated array
        int num_keys = data.size();
        pair<string, string> *data_array = new pair<string, string>[num_keys];
    
        for (int i = 0; i < num_keys; ++i)
        {
            data_array[i] = data[i];
        }
    
        // Create a pair containing the size and pointer to the array
        pair<int, pair<string, string> *> data_pair = {num_keys, data_array};
    
        mtx_sstablelist.lock();
        SSTable_list.push_back(new SSTable(data_pair));
        mtx_sstablelist.unlock();
    
        // Clean up dynamically allocated array
        delete[] data_array;
        return ReturnStatus::SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return ReturnStatus::FAILURE;
    }
    
}

ReturnStatus SET(string &key, string &value)
{
    try
    {
        tree.insert(key, value);
        if (tree.size() == MAX_TREE_SIZE)
        {
            vector<pair<string, string>> data = tree.getSortedPairs();
            create_SSTable(data);
            tree.clear();
        }
        return ReturnStatus::SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return ReturnStatus::FAILURE;
    }
    
}

ReturnStatus DEL(string &key)
{
    try
    {
        string temp = "tombstone";
        SET(key, temp);
        return ReturnStatus::SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return ReturnStatus::FAILURE;
    }
    
}

pair<ReturnStatus, string> GET(strin key)
{
    try
    {
        auto value = tree.find(key);
        if (value.first)
        {
            return make_pair(ReturnStatus::SUCCESS, value.second);
        }
        mtx_sstablelist.lock();
        for (int i = (int)SSTable_list.size() - 1; i >= 0; i--)
        {
            if (SSTable_list[i] == nullptr)
                continue;
            value = SSTable_list[i]->find(key);
            if (value.first)
            {
                mtx_sstablelist.unlock();
                return make_pair(ReturnStatus::SUCCESS, value.second);
            }
        }
    
        mtx_sstablelist.unlock();
        return make_pair(ReturnStatus::SUCCESS, TOMBSTONE);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return make_pair(ReturnStatus::FAILURE, "");
    }
    
}

pair<string, string> *read_SSTable(string &folder_name, int data_size)
{
    auto *data = new pair<string, string>[data_size];

    int idx = 0; // Current index in the array
    for (int i = 0;; i++)
    {
        string file_name = folder_name + "/" + to_string(i) + ".txt";

        if (!fs::exists(file_name))
        {
            break;
        }

        ifstream file(file_name);
        if (!file.is_open())
        {
            cerr << "Error opening file: " << file_name << endl;
            continue;
        }

        string line;
        while (getline(file, line))
        {
            vector<string> tokens = splitString(line);
            for (size_t j = 0; j < tokens.size(); j += 2)
            {
                string _key = tokens[j];
                string _value = (j + 1 < tokens.size()) ? tokens[j + 1] : TOMBSTONE;
                data[idx++] = {_key, _value};
            }
        }

        file.close();
    }

    return data;
}

pair<int, pair<string, string> *> mergeSortedSSTables(const pair<int, pair<string, string> *> &recent_sstable, const pair<int, pair<string, string> *> &old_sstable)
{
    int recent_size = recent_sstable.first;
    int old_size = old_sstable.first;
    pair<string, string> *recent_array = recent_sstable.second;
    pair<string, string> *old_array = old_sstable.second;

    int merged_size = recent_size + old_size;
    pair<string, string> *merged_array = new pair<string, string>[merged_size];

    int i = 0, j = 0, k = 0;

    while (i < recent_size && j < old_size)
    {
        if (recent_array[i].first < old_array[j].first)
        {
            merged_array[k++] = recent_array[i++];
        }
        else if (recent_array[i].first > old_array[j].first)
        {
            merged_array[k++] = old_array[j++];
        }
        else
        {
            merged_array[k++] = recent_array[i++];
            j++;
        }
    }

    while (i < recent_size)
    {
        merged_array[k++] = recent_array[i++];
    }

    while (j < old_size)
    {
        merged_array[k++] = old_array[j++];
    }

    pair<string, string> *resized_array = new pair<string, string>[k];
    for (int idx = 0; idx < k; ++idx)
    {
        resized_array[idx] = merged_array[idx];
    }
    delete[] merged_array;

    return {k, resized_array};
}

void compact()
{
    // cout << " Compaction Thread: I am ready to compact!" << endl;
    while (1)
    {
        int ll = -1, rr = -1;
        bool make_compact = false;
        if (SSTable_list.size() > 1)
        {
            mtx_sstablelist.lock();
            rr = (int)SSTable_list.size() - 1, ll = rr - 1;
            if (SSTable_list[rr] == nullptr)
            {
                SSTable_list.pop_back();
                if (SSTable_list[ll] == nullptr)
                {
                    SSTable_list.pop_back();
                }
            }
            else
            {
                if (SSTable_list[ll] == nullptr)
                {
                    SSTable_list[ll] = SSTable_list[rr];
                    SSTable_list.pop_back();
                }
                else
                {
                    make_compact = true;
                }
            }
            mtx_sstablelist.unlock();
        }
        if (make_compact)
        {
            int num_keys_ll = SSTable_list[ll]->get_num_keys(), num_keys_rr = SSTable_list[rr]->get_num_keys();
            string folder_name_ll = SSTable_list[ll]->get_folder_name(), folder_name_rr = SSTable_list[rr]->get_folder_name();
            pair<string, string> *keyval_ll = read_SSTable(folder_name_ll, num_keys_ll);
            pair<string, string> *keyval_rr = read_SSTable(folder_name_rr, num_keys_rr);

            pair<int, pair<string, string> *> keyval_comp = mergeSortedSSTables(make_pair(num_keys_rr, keyval_rr), make_pair(num_keys_ll, keyval_ll));

            delete[] keyval_ll;
            delete[] keyval_rr;

            mtx_sstablelist.lock();
            delete SSTable_list[ll];
            delete SSTable_list[rr];
            SSTable *table_comp = new SSTable(keyval_comp, folder_name_ll);
            SSTable_list[ll] = table_comp;
            SSTable_list[rr] = nullptr;
            mtx_sstablelist.unlock();

            delete[] keyval_comp.second;
            keyval_comp.second = nullptr;
        }
        usleep(comp_time);
    }
}

void start_compaction()
{

    thread compaction_thread(compact);
    compaction_thread.detach();
}
