#include "CONFIG.h"
#include "REQUEST.h"

ReturnStatus createTable(TableAttr attr)
{
    try
    {
        vector<Partition> partitions = makeOptimalPartitions(attr); // atomically make partitions and save info to metadata
        return ReturnStatus::SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return ReturnStatus::FAILURE;
    }
}

ReturnStatus deleteTable()
{

}

ReturnStatus put(string &key, string &value)
{
    try
    {
        Partition partition_id = consistentHash(key);
        partition_id.put(key,value);
        return ReturnStatus::SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return ReturnStatus::FAILURE;
    }
}

pair<ReturnStatus, string> get(string &key)
{
    try
    {
        Partition partition_id = consistentHash(key);
        string value = partition_id.get(key);
        return make_pair(ReturnStatus::SUCCESS, value);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return make_pair(ReturnStatus::FAILURE, "");
    }
}

vector<pair<string, string>> list(string &lower_bound_key, string &upper_bound_key)
{

}


