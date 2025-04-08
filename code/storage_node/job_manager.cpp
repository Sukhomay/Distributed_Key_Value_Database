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
    JobManager(const int port, const int zone_id) : main_socket_fd(-1), JOB_MANAGER_PORT(port), availabililty_zone_id(zone_id)
    {
        replica_map.clear();
    }
    ~JobManager()
    { 
        if (main_socket_fd != -1)
        {
            close(main_socket_fd);
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
    int main_socket_fd; // Listening socket file descriptor
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
        main_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (main_socket_fd < 0)
            throw runtime_error("ERROR opening socket");

        // Allow socket address reuse
        int opt = 1;
        if (setsockopt(main_socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
            throw runtime_error("setsockopt(SO_REUSEADDR) failed");

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(JOB_MANAGER_PORT);

        if (bind(main_socket_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            throw runtime_error("ERROR on binding");
        if (listen(main_socket_fd, SOMAXCONN) < 0)
            throw runtime_error("Listen error");

        // set_non_blocking(main_socket_fd);
    }

    // Main event loop using epoll to wait for new connection events.
    // Modified event_loop and handle_client functions
    void event_loop()
    {
        int epoll_fd = epoll_create1(0);
        if (epoll_fd < 0)
            throw std::runtime_error("epoll_create1 failed");

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = main_socket_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, main_socket_fd, &ev) < 0)
            throw std::runtime_error("epoll_ctl: main_socket_fd failed");

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
                if (events[i].data.fd == main_socket_fd)
                {
                    // New connection detected on the listening socket.
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(main_socket_fd, (struct sockaddr *)&client_addr, &client_len);
                    if (client_fd < 0)
                    {
                        perror("accept");
                        continue;
                    }
                    // set_non_blocking(client_fd);
                    std::cout << "Accepted connection from "
                              << inet_ntoa(client_addr.sin_addr) << std::endl;

                    // Create a new dedicated socket for the client.
                    int dedicated_fd = socket(AF_INET, SOCK_STREAM, 0);
                    if (dedicated_fd < 0)
                    {
                        perror("socket dedicated");
                        close(client_fd);
                        continue;
                    }
                    int opt = 1;
                    setsockopt(dedicated_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

                    // Bind to an available port (port 0 lets the OS choose a free port).
                    struct sockaddr_in dedicated_addr;
                    memset(&dedicated_addr, 0, sizeof(dedicated_addr));
                    dedicated_addr.sin_family = AF_INET;
                    dedicated_addr.sin_addr.s_addr = INADDR_ANY;
                    dedicated_addr.sin_port = htons(0);
                    if (bind(dedicated_fd, (struct sockaddr *)&dedicated_addr, sizeof(dedicated_addr)) < 0)
                    {
                        perror("bind dedicated socket");
                        close(client_fd);
                        close(dedicated_fd);
                        continue;
                    }

                    // Retrieve the assigned port.
                    socklen_t addrlen = sizeof(dedicated_addr);
                    if (getsockname(dedicated_fd, (struct sockaddr *)&dedicated_addr, &addrlen) < 0)
                    {
                        perror("getsockname");
                        close(client_fd);
                        close(dedicated_fd);
                        continue;
                    }
                    int new_port = ntohs(dedicated_addr.sin_port);

                    // Send new port info to the client.
                    std::string port_msg = "CONNECT TO PORT " + std::to_string(new_port);
                    if (send_all(client_fd, port_msg) == false)
                    {
                        perror("send_all() error");
                        close(client_fd);
                        close(dedicated_fd);
                        continue;
                    }

                    // Wait for OK message from the client.
                    string ok_buffer;
                    if (recv_all(client_fd, ok_buffer) == false)
                    {
                        close(client_fd);
                        close(dedicated_fd);
                        continue;
                    }
                    if (ok_buffer != "OK")
                    {
                        // No valid OK received; terminate.
                        close(client_fd);
                        close(dedicated_fd);
                        continue;
                    }

                    // Close the initial client_fd; further communication will occur on the dedicated socket.
                    close(client_fd);

                    // Spawn a thread to handle communication on the dedicated socket.
                    std::thread client_thread(&JobManager::handle_client, this, dedicated_fd);
                    client_thread.detach();
                }
            }
        }
        close(epoll_fd);
    }

    void handle_client(int dedicated_fd)
    {
        // Start listening on the dedicated socket.
        if (listen(dedicated_fd, 1) < 0)
        {
            perror("listen dedicated socket");
            close(dedicated_fd);
        }

        // Accept connection on the dedicated socket from the client.
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(dedicated_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd < 0)
        {
            perror("accept on dedicated socket");
            close(dedicated_fd);
            return;
        }

        close(dedicated_fd);

        // Send READY message to the client.
        string ready_msg = "READY";
        if (send_all(client_fd, ready_msg) == false)
        {
            perror("send_all() READY message");
            close(client_fd);
            return;
        }

        // Create an epoll instance for the new connection.
        int epoll_fd = epoll_create1(0);
        if (epoll_fd < 0)
        {
            perror("epoll_create1");
            close(client_fd);
            return;
        }

        struct epoll_event ev, events[MAX_EVENTS];
        ev.events = EPOLLIN | EPOLLET; // Edge-triggered mode.
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0)
        {
            perror("epoll_ctl: client_fd");
            close(client_fd);
            close(epoll_fd);
            return;
        }

        bool heartbeat_sent = false;
        // Communication loop: receive client requests and send heartbeat if necessary.
        while (true)
        {
            int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 5000); // 5000ms timeout.
            if (nfds < 0)
            {
                perror("epoll_wait");
                break;
            }
            else if (nfds == 0)
            {
                // Heartbeat sent for the first time and then no response received against it so we close the connection
                if (heartbeat_sent)
                {
                    break;
                }
                // Timeout: send heartbeat to the client.
                std::string heartbeat = HEARTBEAT_MSSG;
                if (send_all(client_fd, heartbeat) == false)
                {
                    perror("send_all() heartbeat");
                    break;
                }
                continue;
            }
            for (int i = 0; i <= nfds; i++)
            {
                if (events[i].data.fd == client_fd)
                {
                    string request;
                    if (recv_all(client_fd, request) == false)
                    {
                        perror("recv_all() from client_fd");
                        // Either client closed the connection or heartbeat reply timed out.
                        close(client_fd);
                        close(epoll_fd);
                        return;
                    }
                    if (request == HEARTBEAT_MSSG)
                    {
                        heartbeat_sent = false;
                        continue;
                    }
                    if (process_request(request, client_fd) == ReturnStatus::FAILURE)
                    {
                        cerr << "Could not process request" << endl;
                        close(client_fd);
                        close(epoll_fd);
                        return;
                    }
                }
            }
        }
        close(client_fd);
        close(epoll_fd);
    }

    // Process a client request command and return a response.
    ReturnStatus process_request(const string &request_str, int client_fd)
    {
        RequestQuery request = RequestQuery::deserialize(request_str);
        if (request.operation == Operation::CREATE_PROPAGATE)
        {
        }
        else if (request.operation == Operation::CREATE)
        {
            // Create a replica for the first time
            ReplyResponse reply;
            reply.status = create_replica(request.request_replica_id, request.sibling_replica_id);
            reply.reponse_replica_id = request.request_replica_id;
            reply.request_id = request.request_id;
            process_reply(reply, client_fd);
            replica_map[request.request_replica_id.slot_id].is_valid = true;
        }
        else
        {
            if (replica_map[request.request_replica_id.slot_id].is_valid == false)
            {
                return ReturnStatus::FAILURE;
            }

            replica_map[request.request_replica_id.slot_id].request_ptr->request.reset();
            ReplyResponse reply;

            if (sem_wait(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
            {
                perror("At JobManager, sem_wait");
                return ReturnStatus::FAILURE;
            }

            replica_map[request.request_replica_id.slot_id].request_ptr->request = request;

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

            reply = replica_map[request.request_replica_id.slot_id].reply_ptr->reply;

            if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
            {
                perror("At JobManager, sem_post");
                return ReturnStatus::FAILURE;
            }

            return process_reply(reply, client_fd);
        }
        return ReturnStatus::SUCCESS;
    }

    // Fork a new process to run the replica machine.
    ReturnStatus create_replica(const ReplicaID own_replica, vector<ReplicaID> &req_sibling_replica)
    {
        if (replica_map.find(own_replica.slot_id) != replica_map.end())
        {
            cerr << "Error at JobManager: Replica already present in the AZ" << endl;
            return ReturnStatus::FAILURE;
        }

        // Initialize the shared memories and semaphores for the replica
        string access_str = JOB_REP_SHM_NAME + to_string(own_replica.slot_id);

        int req_shm_fd, rep_shm_fd;
        if ((req_shm_fd = shm_open((access_str + "req").c_str(), O_CREAT | O_RDWR, 0777)) == -1)
        {
            perror("At JobManager, shm_open");
            return ReturnStatus::FAILURE;
        }
        if ((rep_shm_fd = shm_open((access_str + "rep").c_str(), O_CREAT | O_RDWR, 0777)) == -1)
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

        // To be send: own_replica, sibling_replica
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("At Jobmanager, fork failed while creating Replica");
            return ReturnStatus::FAILURE;
        }
        else if (pid == 0)
        {
            // In child process: execute the replica machine executable.
            SiblingReplica sibling_replica;
            sibling_replica.replicas = req_sibling_replica;
            execl("./storage_node/replica_machine.out", "./storage_node/replica_machine.out", own_replica.serialize().c_str(), sibling_replica.serialize().c_str(), (char *)NULL);
            perror("At Jobmanager, execl failed");
            exit(EXIT_FAILURE);
        }
        return ReturnStatus::SUCCESS;
    }

    ReturnStatus process_reply(const ReplyResponse &reply, int client_fd)
    {
        string response = reply.serialize();
        if (send_all(client_fd, response) == false)
        {
            perror("send_all() reponse");
            return ReturnStatus::FAILURE;
        }
        return ReturnStatus::SUCCESS;
    }
};

int main(int argc, char *argv[])
{
    try
    {
        if (argc < 3)
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
