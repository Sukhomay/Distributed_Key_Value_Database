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

#include "raft_module.cpp" // External functions : print(), onReceiveRaftQuery(), broadcastMessage

class ReplicaMachine
{
public:
    ReplicaMachine(const ReplicaID _replica_id_, vector<ReplicaID> _sibling_replica_id_)
        : replica_id(_replica_id_), sibling_replica_id(_sibling_replica_id_)
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
        raft_machine = new Raft(replica_id, sibling_replica_id.size() + 1, sibling_replica_id, request_ptr, reply_ptr);
    }

    ~ReplicaMachine()
    {
        delete raft_machine;
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
    vector<ReplicaID> sibling_replica_id;
    RequestToReplica *request_ptr;
    ReplyFromReplica *reply_ptr;
    Raft *raft_machine;
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
            raft_machine->onReceiveBroadcastMessage(request_ptr->request);
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

        ReplicaID my_replica_id = ReplicaID::deserialize(argv[1]);
        vector<ReplicaID> sibling_replica_id = SiblingReplica::deserialize(argv[2]).replicas;

        ReplicaMachine replica(my_replica_id, sibling_replica_id);
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
