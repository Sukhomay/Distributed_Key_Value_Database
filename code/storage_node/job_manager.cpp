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

#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>

#include "../db.h"

using namespace std;

#define JOB_MANAGER_PORT 7000
#define MAXLINE 1024
#define MAX_EVENTS 10

class JobManager
{
public:
    JobManager() : listen_fd(-1) {}
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
            cout << "JobManager listening on port " << JOB_MANAGER_PORT << endl;
            event_loop();
        }
        catch (const exception &e)
        {
            cerr << "JobManager exception: " << e.what() << endl;
            throw;
        }
    }

private:
    int listen_fd; // Listening socket file descriptor

    map<string, mqd_t> replica_access; // map of replicas to their respective message queues
    // We make sure that all communications between the job_manager and a replica_machine happens through its message queue only

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
    // The JobManager receives request from the Request Manger of the form
    // <replica_id> <command>
    // It then just passes this message to the corresponding message queue of the replica
    // If a replica is being created for the first time then the message queue is being first created
    // then stored int the map replica_access for further use
    ReturnStatus process_request(const string &request)
    {
        istringstream iss(request);
        string command, replica_id;
        iss >> replica_id >> command;

        if (replica_id.empty() || command.empty())
        {
            cerr << "Error in JobManager: Incomplete request" << endl;
            return ReturnStatus::FAILURE;
        }

        if (command == "CREATE")
        {
            // Create a replica for the first time
        }
        else if (command == "SET")
        {
        }
        else if (command == "DEL")
        {
        }
        else if (command == "GET")
        {
        }
        else
        {
            cerr << "Error in JobManager: Invalid command" << endl;
            return ReturnStatus::FAILURE;
        }
        return ReturnStatus::SUCCESS;
    }

    // Fork a new process to run the replica machine.
    string create_replica(const string &replica_id)
    {
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork failed");
            return "ERROR: Fork failed\r\n";
        }
        else if (pid == 0)
        {
            // In child process: execute the replica machine executable.
            execl("./replica_machine", "./replica_machine", replica_id.c_str(), (char *)NULL);
            perror("execl failed");
            exit(EXIT_FAILURE);
        }
        else
        {
            ostringstream oss;
            oss << "Replica created with PID " << pid << "\r\n";
            return oss.str();
        }
    }
};

int main()
{
    try
    {
        JobManager manager;
        manager.run();
    }
    catch (const exception &e)
    {
        cerr << "JobManager terminated with error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
