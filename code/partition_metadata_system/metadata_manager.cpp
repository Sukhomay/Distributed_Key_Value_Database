#include "../db.h"

vector<Address> metadata_managers;

struct Partition
{
    PartitionID partitionId;
    vector<ReplicaID> replicas;
};
struct Table
{
    TableID tableId;
    vector<Partition> partitions;
};

map<tableID, Table> tables;
class MetadataManager
{
    void recvCreateTable(int numPartitions, int numReplicas)
    {
        // Need to create a table with numPartitions partitions
        // Get a list of partitionsIds
        vector<PartitionID> partitionIds = getPartitionIds(numPartitions);

        // Get a list of numReplicas replicas for each partition
        map<PartitionID, vector<ReplicaID>> partitionReplicas;
        for (int i = 0; i < numPartitions; i++)
        {
            vector<ReplicaID> replicas = getReplicasForPartition(i, numReplicas);
            partitionReplicas[i] = replicas;
        }

        // Send success to Request Manager
        // Create table :: SUCCESS
        return EXIT_SUCCESS;
    }

    void recvDropTable(int tableId)
    {
        // Need to drop a table with tableId



        // Send success to Request Manager
        // Drop table :: SUCCESS
        return EXIT_SUCCESS;
    }

    void 


};