#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <thread>
#include <vector>
#include <map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <semaphore.h>

#include "../db.h"

using namespace std;

typedef struct ReplicaAccess
{
    RequestToReplica *request_ptr;
    ReplyFromReplica *reply_ptr;
    sem_t sem_access;
    bool is_valid = false;
} ReplicaAccess;

class JobManager
{
public:
    JobManager(const int port, const int zone_id) : 
    listen_fd(-1), JOB_MANAGER_PORT(port), availabililty_zone_id(zone_id) {}
    ~JobManager()
    {
        if (listen_fd != -1)
        {
            close(listen_fd);
        }
    }

    // Start the JobManager server loop.
    void run()
    {
        try
        {
            init_server();
            cout << "JobManager for AZ" << availabililty_zone_id << " listening on port " << JOB_MANAGER_PORT << endl;
            event_loop();
        }
        catch (const exception &e)
        {
            cerr << "JobManager exception: " << e.what() << endl;
            throw;
        }
    }

private:
    const int availabililty_zone_id;
    const int JOB_MANAGER_PORT;
    int listen_fd; // Listening socket file descriptor

    map<int, ReplicaAccess> replica_map;

    // Set a file descriptor to non-blocking mode.
    void set_non_blocking(int fd)
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1)
            throw runtime_error("fcntl(F_GETFL) failed");
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
            throw runtime_error("fcntl(F_SETFL) failed");
    }

    // Initialize the listening socket.
    void init_server()
    {
        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0)
            throw runtime_error("ERROR opening socket");

        // Allow socket address reuse
        int opt = 1;
        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
            throw runtime_error("setsockopt(SO_REUSEADDR) failed");

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(JOB_MANAGER_PORT);

        if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            throw runtime_error("ERROR on binding");
        if (listen(listen_fd, SOMAXCONN) < 0)
            throw runtime_error("Listen error");

        set_non_blocking(listen_fd);
    }

    // Main event loop using epoll to wait for new connection events.
    void event_loop()
    {
        int epoll_fd = epoll_create1(0);
        if (epoll_fd < 0)
            throw runtime_error("epoll_create1 failed");

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0)
            throw runtime_error("epoll_ctl: listen_fd failed");

        struct epoll_event events[MAX_EVENTS];

        while (true)
        {
            int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
            if (nfds < 0)
            {
                perror("epoll_wait");
                continue;
            }
            for (int i = 0; i < nfds; i++)
            {
                if (events[i].data.fd == listen_fd)
                {
                    // New connection detected on the listening socket.
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
                    if (client_fd < 0)
                    {
                        perror("accept");
                        continue;
                    }
                    set_non_blocking(client_fd);
                    cout << "Accepted connection from "
                         << inet_ntoa(client_addr.sin_addr) << endl;

                    // For each new connection, spawn a thread to handle it.
                    thread client_thread(&JobManager::handle_client, this, client_fd);
                    client_thread.detach();
                }
            }
        }
        close(epoll_fd);
    }

    // Function to handle client requests in a dedicated thread using epoll.
    void handle_client(int client_fd)
    {
        // Create an epoll instance for the client.
        int client_epoll_fd = epoll_create1(0);
        if (client_epoll_fd < 0)
        {
            perror("epoll_create1 in client");
            close(client_fd);
            return;
        }
        struct epoll_event ev, events[MAX_EVENTS];
        ev.events = EPOLLIN | EPOLLET; // Use edge-triggered mode.
        ev.data.fd = client_fd;
        if (epoll_ctl(client_epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0)
        {
            perror("epoll_ctl: client_fd");
            close(client_fd);
            close(client_epoll_fd);
            return;
        }

        char buffer[MAXLINE];
        while (true)
        {
            int nfds = epoll_wait(client_epoll_fd, events, MAX_EVENTS, 5000); // 5000ms timeout.
            if (nfds < 0)
            {
                perror("epoll_wait client");
                break;
            }
            else if (nfds == 0)
            {
                // Timeout occurred; you could implement a heartbeat or simply continue.
                continue;
            }
            for (int i = 0; i < nfds; i++)
            {
                if (events[i].data.fd == client_fd)
                {
                    int n = read(client_fd, buffer, MAXLINE - 1);
                    if (n <= 0)
                    {
                        if (n < 0)
                            perror("read client");
                        close(client_fd);
                        close(client_epoll_fd);
                        return;
                    }
                    buffer[n] = '\0';
                    string request(buffer);
                    string response = process_request(request);
                    write(client_fd, response.c_str(), response.size());
                }
            }
        }
        close(client_fd);
        close(client_epoll_fd);
    }

    // Process a client request command and return a response.
    ReturnStatus process_request(const string &request_str)
    {
        RequestQuery request = deserializeRequestQuery(request_str);
        if (request.operation == Operation::CREATE)
        {
            // Create a replica for the first time
            replica_map[request.request_replica_id.slot_id].is_valid = false;
            return create_replica(request.request_replica_id, request.other_replica_id);
        }
        else
        {
            if (replica_map[request.request_replica_id.slot_id].is_valid == false)
            {
                return ReturnStatus::FAILURE;
            }
            if (request.operation == Operation::SET)
            {
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }

                replica_map[request.request_replica_id.slot_id].request_ptr->op = Operation::SET;
                replica_map[request.request_replica_id.slot_id].request_ptr->key_len = static_cast<size_t>((int)request.key.size() + 1);
                memcpy(replica_map[request.request_replica_id.slot_id].request_ptr->key, request.key.c_str(), replica_map[request.request_replica_id.slot_id].request_ptr->key_len);
                replica_map[request.request_replica_id.slot_id].request_ptr->val_len = static_cast<size_t>((int)request.value.size() + 1);
                memcpy(replica_map[request.request_replica_id.slot_id].request_ptr->val, request.value.c_str(), replica_map[request.request_replica_id.slot_id].request_ptr->val_len);
                if (sem_post(&replica_map[request.request_replica_id.slot_id].request_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].reply_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }
                ReplyResponse reply;
                reply.reponse_replica_id = request.request_replica_id;
                reply.status = static_cast<int>(replica_map[request.request_replica_id.slot_id].reply_ptr->status);

                if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }

                return process_reply(reply);
            }
            else if (request.operation == Operation::DEL)
            {
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }

                replica_map[request.request_replica_id.slot_id].request_ptr->op = Operation::DEL;
                replica_map[request.request_replica_id.slot_id].request_ptr->key_len = static_cast<size_t>((int)request.key.size() + 1);
                memcpy(replica_map[request.request_replica_id.slot_id].request_ptr->key, request.key.c_str(), replica_map[request.request_replica_id.slot_id].request_ptr->key_len);
                if (sem_post(&replica_map[request.request_replica_id.slot_id].request_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].reply_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }
                ReplyResponse reply;
                reply.reponse_replica_id = request.request_replica_id;
                reply.status = static_cast<int>(replica_map[request.request_replica_id.slot_id].reply_ptr->status);

                if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }

                return process_reply(reply);
            }
            else if (request.operation == Operation::GET)
            {
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }

                replica_map[request.request_replica_id.slot_id].request_ptr->op = Operation::GET;
                replica_map[request.request_replica_id.slot_id].request_ptr->key_len = static_cast<size_t>((int)request.key.size() + 1);
                memcpy(replica_map[request.request_replica_id.slot_id].request_ptr->key, request.key.c_str(), replica_map[request.request_replica_id.slot_id].request_ptr->key_len);
                if (sem_post(&replica_map[request.request_replica_id.slot_id].request_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }
                if (sem_wait(&replica_map[request.request_replica_id.slot_id].reply_ptr->sem) == -1)
                {
                    perror("At JobManager, sem_wait");
                    return ReturnStatus::FAILURE;
                }
                ReplyResponse reply;
                reply.reponse_replica_id = request.request_replica_id;
                reply.status = static_cast<int>(replica_map[request.request_replica_id.slot_id].reply_ptr->status);
                reply.value.assign(replica_map[request.request_replica_id.slot_id].reply_ptr->val, replica_map[request.request_replica_id.slot_id].reply_ptr->val_len);

                if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
                {
                    perror("At JobManager, sem_post");
                    return ReturnStatus::FAILURE;
                }

                return process_reply(reply);
            }
            else
            {
                cerr << "Error in JobManager: Invalid command" << endl;
                return ReturnStatus::FAILURE;
            }
        }
        return ReturnStatus::SUCCESS;
    }

    // Fork a new process to run the replica machine.
    ReturnStatus create_replica(const ReplicaID own_replica, vector<ReplicaID> &other_replica)
    {
        if (replica_map.find(own_replica.slot_id) != replica_map.end())
        {
            cerr << "Error at JobManager: Replica already present in the AZ" << endl;
            return ReturnStatus::FAILURE;
        }

        // Initialize the shared memories and semaphores for the replica
        string access_str = JOB_REP_SHM_NAME + to_string(own_replica.slot_id);

        int req_shm_fd, rep_shm_fd;
        if (req_shm_fd = shm_open((access_str + "req").c_str(), O_CREAT | O_RDWR, 0777) == -1)
        {
            perror("At JobManager, shm_open");
            return ReturnStatus::FAILURE;
        }
        if (req_shm_fd = shm_open((access_str + "req").c_str(), O_CREAT | O_RDWR, 0777) == -1)
        {
            perror("At JobManager, shm_open");
            return ReturnStatus::FAILURE;
        }

        if (ftruncate(req_shm_fd, sizeof(RequestToReplica)) == -1)
        {
            perror("At JobManager, ftruncate");
            return ReturnStatus::FAILURE;
        }
        if (ftruncate(rep_shm_fd, sizeof(ReplyFromReplica)) == -1)
        {
            perror("At JobManager, ftruncate");
            return ReturnStatus::FAILURE;
        }

        // Map the shared memory region.
        void *req_ptr = mmap(nullptr, sizeof(RequestToReplica), PROT_READ | PROT_WRITE, MAP_SHARED, req_shm_fd, 0);
        if (req_ptr == MAP_FAILED)
        {
            perror("At JobManager, mmap");
            return ReturnStatus::FAILURE;
        }
        replica_map[own_replica.slot_id].request_ptr = static_cast<RequestToReplica *>(req_ptr);
        void *rep_ptr = mmap(nullptr, sizeof(RequestToReplica), PROT_READ | PROT_WRITE, MAP_SHARED, rep_shm_fd, 0);
        if (rep_ptr == MAP_FAILED)
        {
            perror("At JobManager, mmap");
            return ReturnStatus::FAILURE;
        }
        replica_map[own_replica.slot_id].reply_ptr = static_cast<ReplyFromReplica *>(rep_ptr);

        // Initialize the POSIX semaphore for process sharing with an initial value of 1.
        if (sem_init(&replica_map[own_replica.slot_id].request_ptr->sem, 1, 0) == -1)
        {
            perror("At JobManager, sem_init");
            return ReturnStatus::FAILURE;
        }
        if (sem_init(&replica_map[own_replica.slot_id].reply_ptr->sem, 1, 0) == -1)
        {
            perror("At JobManager, sem_init");
            return ReturnStatus::FAILURE;
        }

        if (sem_init(&replica_map[own_replica.slot_id].sem_access, 1, 1) == -1)
        {
            perror("At JobManager, sem_init");
            return ReturnStatus::FAILURE;
        }

        // To be send: own_replica, other_replica
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("At Jobmanager, fork failed while creating Replica");
            return ReturnStatus::FAILURE;
        }
        else if (pid == 0)
        {
            // In child process: execute the replica machine executable.
            execl("./replica_machine", "./replica_machine", serializeReplicaID(own_replica).c_str(), serializeReplicaIDVector(other_replica).c_str(), (char *)NULL);
            perror("At Jobmanager, execl failed");
            exit(EXIT_FAILURE);
        }
        replica_map[own_replica.slot_id].is_valid = true;
        return ReturnStatus::SUCCESS;
    }

    ReturnStatus process_reply(const ReplyResponse &reply)
    {
    }
};

int main(int argc, char *argv[])
{
    try
    {
        if(argc<3)
        {
            cerr << "Error at JobManager: incomplete arguments" << endl;
            return EXIT_FAILURE;
        }

        int availability_zone_id = stoi(argv[1]);
        int port = stoi(argv[2]);
        JobManager manager(port, availability_zone_id);
        manager.run();
    }
    catch (const exception &e)
    {
        cerr << "JobManager terminated with error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
