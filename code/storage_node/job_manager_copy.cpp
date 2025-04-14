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

#include "../db_copy.h"

using namespace std;

typedef struct ReplicaAccess
{
    map<ReplicaID, Address> replica_addr_map;
    pid_t pid;
    string commfile;
    int commfd;
    sem_t sem_comm;
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

                    cout << ok_buffer << endl;

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
            int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 500000); // 5000ms timeout.
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
                cout << "HeartBeat mssg sent" << endl;
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
        ReplicaID request_replica_id = request.request_replica_id;
        int request_slot_id = request.request_replica_id.slot_id;
        ReplyResponse reply;

        if (request.operation == Operation::CREATE_PROPAGATE || request.operation == Operation::CREATE)
        {
            // Create a replica for the first time
            reply.status = create_replica(request.request_replica_id);

            // Wait for replica Machine to give its address info
            if (sem_wait(&(replica_map[request_slot_id].sem_comm)) == -1)
            {
                perror("At JobManager, sem_wait");
                return ReturnStatus::FAILURE;
            }
            
            string addr_str =  read_first_line(replica_map[request_slot_id].commfd);
            Address request_replica_addr = Address::deserialize(addr_str);
            replica_map[request_slot_id].replica_addr_map[request_replica_id] = request_replica_addr;
            
            string complete_map_str;
            if (request.operation == Operation::CREATE_PROPAGATE)
            {
                // Ask other AZs to create the sibling replicas
                request.sibling_replica_id.push_back(request.request_replica_id);
                RequestQuery sibling_request = request;
                sibling_request.operation = Operation::CREATE;
                vector<int> sibling_sockfd_list;

                for (auto replica_id : request.sibling_replica_id)
                {
                    if (replica_id == request.request_replica_id)
                        continue;

                    sibling_request.request_replica_id = replica_id;
                    vector<ReplicaID> sibling_sibling_replica_id;
                    for (auto rr : request.sibling_replica_id)
                    {
                        if (rr == replica_id)
                            continue;
                        sibling_sibling_replica_id.push_back(rr);
                    }
                    sibling_request.sibling_replica_id = sibling_sibling_replica_id;

                    Address sibling_addr = getReplicaAddr(replica_id);

                    pair<ReplyResponse, int> res = sendInitialRequest(sibling_addr, sibling_request);

                    sibling_sockfd_list.push_back(res.second);
                    ReplyResponse &response = res.first;

                    Address sibling_addr = Address::deserialize(response.value);
                    replica_map[request_slot_id].replica_addr_map[replica_id] = sibling_addr;
                }
                request.sibling_replica_id.pop_back();

                cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
                cout << "Replica Map created at Propagator:" << endl;
                for (auto &item : replica_map[request.request_replica_id.slot_id].replica_addr_map)
                {
                    item.first.print();
                    cout << " -> ";
                    item.second.print();
                    cout << endl;
                }
                cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
                // Now I have the entire map info with me (propagator)
                // I have to give this map to my replica
                // Finally I have to send this map to every sibling
                ReplyResponse response;
                complete_map_str = serializeReplicaAddrMap(replica_map[request_slot_id].replica_addr_map);
                response.value = complete_map_str;
                for (auto sibling_sockfd : sibling_sockfd_list)
                {
                    send_all(sibling_sockfd, response.serialize());

                    close(sibling_sockfd);
                }
            }
            else
            {
                // In case of create I just to reply with a status and with the my own replica address info to the Propagator
                // Then I have to wait for the Propagator to send address info of all other siblings

                // Send the status and my own replica address to the propagator
                reply.reponse_replica_id = request_replica_id;
                reply.request_id = request.request_id;
                reply.value = replica_map[request_slot_id].replica_addr_map[request_replica_id].serialize();

                process_reply(reply, client_fd);

                // Now I wait for the propagator to send me the complete map
                string response_str;
                recv_all(client_fd, response_str);
                ReplyResponse response = ReplyResponse::deserialize(response_str);

                // response.print();
                complete_map_str = response.value;
                replica_map[request_slot_id].replica_addr_map = deserializeReplicaAddrMap(complete_map_str);
            }
            // Provide the replica machine with info of all its sibling to my own repplica
            write_first_line(replica_map[request_slot_id].commfd, complete_map_str);

            if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_comm) == -1)
            {
                perror("At JobManager, sem_post");
                return ReturnStatus::FAILURE;
            }
            reply.status = ReturnStatus::SUCCESS;
            replica_map[request_slot_id].is_valid = true;
        }
        else
        {
            if (replica_map[request.request_replica_id.slot_id].is_valid == false)
            {
                return ReturnStatus::FAILURE;
            }

            if (sem_wait(&replica_map[request_slot_id].sem_access) == -1)
            {
                perror("At JobManager, sem_wait");
                return ReturnStatus::FAILURE;
            }

            if(request.operation == Operation::SET || request.operation == Operation::GET || request.operation == Operation::DEL)
            {
                write_first_line(replica_map[request_slot_id].commfd, request_str);
            }
            else
            {
                cerr << "Unknown command" << endl;
            }

            if (sem_post(&replica_map[request_slot_id].sem_comm) == -1)
            {
                perror("At JobManager, sem_post");
                return ReturnStatus::FAILURE;
            }

            cout << "Have sent request; Waiting for reply" << endl;

            if (sem_wait(&replica_map[request_slot_id].sem_comm) == -1)
            {
                perror("At JobManager, sem_wait");
                return ReturnStatus::FAILURE;
            }
            string reply_str = read_first_line(replica_map[request_slot_id].commfd);
            reply = ReplyResponse::deserialize(reply_str);

            if (sem_post(&replica_map[request.request_replica_id.slot_id].sem_access) == -1)
            {
                perror("At JobManager, sem_post");
                return ReturnStatus::FAILURE;
            }
        }
        // return ReturnStatus::SUCCESS;
        return process_reply(reply, client_fd);
    }

    // Fork a new process to run the replica machine.
    ReturnStatus create_replica(const ReplicaID replica_id)
    {
        if (replica_map.find(replica_id.slot_id) != replica_map.end())
        {
            cerr << "Error at JobManager: Replica already present in the AZ" << endl;
            return ReturnStatus::FAILURE;
        }

        if (sem_init(&replica_map[replica_id.slot_id].sem_access, 1, 1) == -1)
        {
            perror("At JobManager, sem_init");
            return ReturnStatus::FAILURE;
        }

        if (sem_init(&replica_map[replica_id.slot_id].sem_comm, 1, 1) == -1)
        {
            perror("At JobManager, sem_init");
            return ReturnStatus::FAILURE;
        }

        string commfile = "commfile_" + to_string(replica_id.availability_zone_id) + "_" + to_string(replica_id.slot_id);

        // To be send: replica_id, sibling_replica
        // std::cout << "Preparing to fork..." << std::endl;
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("At Jobmanager, fork failed while creating Replica");
            return ReturnStatus::FAILURE;
        }
        else if (pid == 0)
        {
            // In child process: execute the replica machine executable

            // open a file for to communicate with replica
            int fd = open(commfile.c_str(), O_RDWR | O_CREAT, 0777);
            if (fd < 0)
            {
                perror("open");
                return ReturnStatus::FAILURE;
            }

            replica_map[replica_id.slot_id].commfile = commfile;
            replica_map[replica_id.slot_id].commfd = fd;

            execl("./storage_node/replica_machine.out", "./storage_node/replica_machine.out", replica_id.serialize().c_str(), (char *)NULL);
            perror("At Jobmanager, execl failed");
            exit(EXIT_FAILURE);
        }
        else
        {
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

        // // Open the output file in write mode. This creates the file if it doesn't exist
        // // and truncates it to zero length if it already exists.
        // std::ofstream file("job_manager_output.txt", std::ios::out | std::ios::trunc);
        // if (!file.is_open())
        // {
        //     std::cerr << "Error: Could not open output.txt for writing." << std::endl;
        //     return 1;
        // }

        // // Save the original stream buffer of cout.
        // std::streambuf *originalCoutBuffer = std::cout.rdbuf();

        // // Redirect cout's output to the file.
        // std::cout.rdbuf(file.rdbuf());

        // Register the SIGINT signal handler.
        // struct sigaction sa;
        // sa.sa_handler = signal_handler;
        // sigemptyset(&sa.sa_mask);
        // sa.sa_flags = 0; // You might use SA_RESTART if needed.
        // if (sigaction(SIGINT, &sa, nullptr) == -1)
        // {
        //     std::cerr << "Failed to set signal handler: " << std::strerror(errno) << std::endl;
        //     return 1;
        // }

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
