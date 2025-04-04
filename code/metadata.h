#include <bits/stdc++.h>
using namespace std;
enum class ReturnStatus {
    FAILURE = 0,
    SUCCESS = 1
};

struct TableAttr {
    string table_name;
    int RCU;
    int WCU;
    int num_replica;
}


class Partition {
    private:
        vector<int> replication_group;
        int num_repica;
        int leader_replica;
    public:
        void put(string &key, string &value){

        }
        string get(string &key){
            
        }
}