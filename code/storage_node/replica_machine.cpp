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
        string access_str = JOB_REP_SHM_NAME + replica_id.serialize();

        int req_shm_fd, rep_shm_fd, addr_map_shm_fd;
        if ((req_shm_fd = shm_open((access_str + "req").c_str(), O_RDWR, 0777)) == -1)
        {
            perror("At ReplicaMachine, shm_open");
        }
        if ((rep_shm_fd = shm_open((access_str + "rep").c_str(), O_RDWR, 0777)) == -1)
        {
            perror("At ReplicaMachine, shm_open");
        }
        if ((addr_map_shm_fd = shm_open((access_str + "addr_map").c_str(), O_RDWR, 0777)) == -1)
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
        if (ftruncate(addr_map_shm_fd, sizeof(ReplicaInfo)) == -1)
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

        void *rep_ptr = mmap(nullptr, sizeof(ReplyFromReplica), PROT_READ | PROT_WRITE, MAP_SHARED, rep_shm_fd, 0);
        if (rep_ptr == MAP_FAILED)
        {
            perror("At ReplicaMachine, mmap");
        }
        reply_ptr = static_cast<ReplyFromReplica *>(rep_ptr);

        void *addr_map_shm_ptr = mmap(nullptr, sizeof(ReplicaInfo), PROT_READ | PROT_WRITE, MAP_SHARED, addr_map_shm_fd, 0);
        if (addr_map_shm_ptr == MAP_FAILED)
        {
            perror("At ReplicaMachine, mmap");
        }
        addr_map_ptr = static_cast<ReplicaInfo *>(addr_map_shm_ptr);

        // Set up the LSM Ttree
        // raft_machine = new Raft(replica_id, sibling_replica_id.size() + 1, sibling_replica_id, request_ptr, reply_ptr);
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
            // Create a TCP socket
            Address replica_addr = getReplicaAddr(replica_id);
            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0)
            {
                perror("socket creation failed");
                exit(EXIT_FAILURE);
            }

            // Prepare the sockaddr_in structure using the host and port in replica_addr.
            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            // Convert the host string to a network address structure.
            if (inet_pton(AF_INET, replica_addr.host.c_str(), &addr.sin_addr) <= 0)
            {
                perror("inet_pton failed");
                close(sockfd);
                exit(EXIT_FAILURE);
            }
            // Set the port (convert from host to network byte order).
            addr.sin_port = 0;

            // Bind the socket to the specified address.
            if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
            {
                perror("bind failed");
                close(sockfd);
                exit(EXIT_FAILURE);
            }

            // Retrieve the bound socket details.
            socklen_t addrLen = sizeof(addr);
            if (getsockname(sockfd, (struct sockaddr *)&addr, &addrLen) < 0)
            {
                perror("getsockname failed");
                close(sockfd);
                exit(EXIT_FAILURE);
            }

            // Convert the port from network byte order and store in node_port.
            int node_port = ntohs(addr.sin_port);
            cout << "Socket bound to : <" << replica_addr.host << ", " << node_port << ">" << endl;
            replica_addr_map[replica_id] = Address{replica_addr.host, node_port};
            addr_map_ptr->serialize_map(replica_addr_map);
            if (sem_post(&(addr_map_ptr->sem_replica_to_jobmanager)) == -1)
            {
                perror("At Replica_machine, sem_post");
                return EXIT_FAILURE;
            }
            if (sem_wait(&(addr_map_ptr->sem_jobmanager_to_replica)) == -1)
            {
                perror("At ReplicaMachine, sem_wait");
                return EXIT_FAILURE;
            }
            replica_addr_map = addr_map_ptr->deserialize_map();

            cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
            cout << "Replica Map received at ReplicaMachine "; replica_id.print(); cout << endl;
            for (auto &item : replica_addr_map)
            {
                item.first.print();
                cout << " -> ";
                item.second.print();
                cout << endl;
            }
            cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;

            return run(sockfd);
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
    map<ReplicaID, Address> replica_addr_map;
    ReplicaInfo *addr_map_ptr;
    Raft *raft_machine;
    ReplicaInfo replica_info;
    // Main event loop (to be implemented)
    int run(int sockfd)
    {
        raft_machine = new Raft(replica_id, sibling_replica_id.size() + 1, sibling_replica_id, reply_ptr, sockfd, replica_addr_map);
        while (true)
        {
            // wait for the request to come
            if (sem_wait(&(request_ptr->sem)) == -1)
            {
                perror("At ReplicaMachine, sem_wait");
                return EXIT_FAILURE;
            }
            raft_machine->onReceiveUserRequest(request_ptr->request);
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
