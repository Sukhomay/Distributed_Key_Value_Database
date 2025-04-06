#include <iostream>
#include <stdexcept>
#include <string>
#include <cstring>
#include <cstdlib>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using namespace std;

#include "lsm.cpp" // External functions: start_compaction(), SET(), GET(), DEL()

class ReplicaMachine
{
public:
    ReplicaMachine(const ReplicaID _replica_id_, vector<ReplicaID> _other_replica_id_)
        : replica_id(_replica_id_), other_replica_id(_other_replica_id_)
    {
        // Setup the shared memories for the replica
        string access_str = JOB_REP_SHM_NAME + to_string(replica_id.slot_id);

        int req_shm_fd, rep_shm_fd;
        if ((req_shm_fd = shm_open((access_str + "req").c_str(), O_RDWR, 0777)) == -1)
        {
            perror("At ReplicaMachine, shm_open");
        }
        if ((rep_shm_fd = shm_open((access_str + "rep").c_str(), O_RDWR, 0777)) == -1)
        {
            perror("At ReplicaMachine, shm_open");
        }

        if (ftruncate(req_shm_fd, sizeof(RequestToReplica)) == -1)
        {
            perror("At ReplicaMachine, ftruncate");
        }
        if (ftruncate(rep_shm_fd, sizeof(ReplyFromReplica)) == -1)
        {
            perror("At ReplicaMachine, ftruncate");
        }

        // Map the shared memory region.
        void *req_ptr = mmap(nullptr, sizeof(RequestToReplica), PROT_READ | PROT_WRITE, MAP_SHARED, req_shm_fd, 0);
        if (req_ptr == MAP_FAILED)
        {
            perror("At ReplicaMachine, mmap");
        }
        request_ptr = static_cast<RequestToReplica *>(req_ptr);
        void *rep_ptr = mmap(nullptr, sizeof(RequestToReplica), PROT_READ | PROT_WRITE, MAP_SHARED, rep_shm_fd, 0);
        if (rep_ptr == MAP_FAILED)
        {
            perror("At ReplicaMachine, mmap");
        }
        reply_ptr = static_cast<ReplyFromReplica *>(rep_ptr);

        // Set up the LSM Ttree
        mt = new MergeTree(to_string(replica_id.availability_zone_id) + "_" + to_string(replica_id.slot_id));
    }

    ~ReplicaMachine()
    {
        delete mt;
    }

    // Start the server loop
    int start()
    {
        try
        {
            cout << "Replica Machine (" << replica_id.availability_zone_id << ", " << replica_id.slot_id << ") running..." << endl;
            return run();
        }
        catch (const exception &e)
        {
            cerr << "Replica Machine (" << replica_id.availability_zone_id << ", " << replica_id.slot_id << "):: Exception in start(): " << e.what() << endl;
            throw;
        }
    }

private:
    ReplicaID replica_id;
    vector<ReplicaID> other_replica_id;
    RequestToReplica *request_ptr;
    ReplyFromReplica *reply_ptr;
    MergeTree *mt;

    // Main event loop (to be implemented)
    int run()
    {
        while (true)
        {
            // wait for the request to come
            if (sem_wait(&(request_ptr->sem)) == -1)
            {
                perror("At ReplicaMachine, sem_wait");
                return EXIT_FAILURE;
            }
            // Attend the request and respond accordingly;
            if (request_ptr->op == Operation::SET)
            {
                string key(request_ptr->key, request_ptr->key_len);
                string val(request_ptr->val, request_ptr->val_len);

                ReturnStatus status = mt->SET(key, val);
                
                reply_ptr->status = status;
                if(sem_post(&(reply_ptr->sem)) == -1)
                {
                    perror("At ReplicaMachine, sem_postt");
                    return EXIT_FAILURE;
                }

            }
            else if (request_ptr->op == Operation::DEL)
            {
                string key(request_ptr->key, request_ptr->key_len);
                
                ReturnStatus status = mt->DEL(key);

                reply_ptr->status = status;
                if(sem_post(&(reply_ptr->sem)) == -1)
                {
                    perror("At ReplicaMachine, sem_postt");
                    return EXIT_FAILURE;
                }
            }
            else if (request_ptr->op == Operation::GET)
            {
                string key(request_ptr->key, request_ptr->key_len);
                
                pair<ReturnStatus, string> result = mt->GET(key);

                reply_ptr->status = result.first;
                reply_ptr->val_len = static_cast<size_t>((int)result.second.size() + 1);
                memcpy(reply_ptr->val, result.second.c_str(), reply_ptr->val_len);
                if(sem_post(&(reply_ptr->sem)) == -1)
                {
                    perror("At ReplicaMachine, sem_postt");
                    return EXIT_FAILURE;
                }

            }
            else
            {
                cerr << "Invalid Operation at ReplicaMachine" << endl;
                return EXIT_FAILURE;
            }
        }
        return EXIT_SUCCESS;
    }
};

// -------------------------------------------------------------------------
// Driver Function
int main(int argc, char *argv[])
{
    try
    {
        if (argc < 3)
        {
            cerr << "Error at ReplicaMachine: not sufficient arguments provided" << endl;
            return EXIT_FAILURE;
        }

        ReplicaID my_replica_id = deserializeReplicaID(argv[1]);
        vector<ReplicaID> other_replica_id = deserializeReplicaIDVector(argv[2]);

        ReplicaMachine replica(my_replica_id, other_replica_id);
        return replica.start();
    }
    catch (const std::exception &e)
    {
        cerr << "ReplicaMachine terminated with error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
// -------------------------------------------------------------------------
