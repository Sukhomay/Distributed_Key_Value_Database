#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <cstdlib>

#define START_PORT 5000
#define NUM_PROCESSES 10
#define BACKLOG 5  // Maximum number of pending connections

void start_server(int port) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_size = sizeof(client_addr);
    char buffer[1024];

    // Create socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Configure server address structure
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;  // Bind to all interfaces
    server_addr.sin_port = htons(port);

    // Bind socket to the port
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    // Start listening for connections
    if (listen(server_fd, BACKLOG) == -1) {
        perror("Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server started on port " << port << std::endl;

    while (true) {
        // Accept a client connection
        client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_size);
        if (client_fd == -1) {
            perror("Accept failed");
            continue;
        }

        // Print client connection info
        std::cout << "Client connected on port " << port << std::endl;

        // Send a response
        const char* message = "Hello from server\n";
        send(client_fd, message, strlen(message), 0);

        // Close the client connection
        close(client_fd);
    }

    // Close server socket
    close(server_fd);
}

int main() {
    for (int i = 0; i < NUM_PROCESSES; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("Fork failed");
            exit(EXIT_FAILURE);
        } else if (pid == 0) {
            // Child process: Start a server on port (START_PORT + i)
            start_server(START_PORT + i);
            exit(0); // Exit to prevent child from continuing loop
        }
    }

    // Parent process: Wait for child processes to run indefinitely
    while (true) {
        sleep(1);
    }

    return 0;
}
