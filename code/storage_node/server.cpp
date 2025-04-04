#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/select.h>
#include <dlfcn.h>
#include <errno.h>

#define MAXLINE 1024
#define PORT 6379
#define MAX_CLIENTS 10000
fd_set master_fds;

extern void start_compaction();
extern void SET(char *, char *);
extern void DEL(char *);
extern const char *GET(char *);

ssize_t send_message(int sockfd, const char *message, size_t length)
{
    ssize_t total_sent = 0; // Total bytes sent so far
    ssize_t bytes_sent;

    while (total_sent < length)
    {
        bytes_sent = send(sockfd, message + total_sent, length - total_sent, 0);
        if (bytes_sent < 0)
        {
            perror("Send failed");
            return -1;
        }
        total_sent += bytes_sent;
    }

    return total_sent;
}

ssize_t receive_message(int sockfd, char *buffer, size_t buffer_size)
{
    ssize_t total_received = 0; // Total bytes received so far
    ssize_t bytes_received;

    while (total_received < buffer_size - 1)
    {
        bytes_received = recv(sockfd, buffer + total_received, buffer_size - 1 - total_received, 0);
        if (bytes_received < 0)
        {
            break;
        }
        if (bytes_received == 0)
        {
            // Connection closed by the peer
            FD_CLR(sockfd, &master_fds);
            break;
        }
        total_received += bytes_received;
    }

    buffer[total_received] = '\0'; // Null-terminate the buffer
    return total_received;
}

// Initialize the database functions from shared library
// void init_db() {
//     libdb = dlopen("./attachments/libdatabase.so", RTLD_LAZY);  // Adjust the path to your shared library
//     if (!libdb) {
//         fprintf(stderr, "Failed to load shared library\n");
//         exit(EXIT_FAILURE);
//     }
//     SET = (db_set)dlsym(libdb, "SET");
//     GET = (db_get)dlsym(libdb, "GET");
//     DEL = (db_del)dlsym(libdb, "DEL");
//     // start_compaction = (start_compaction_thread)dlsym(libdb, "start_compaction");

//     // Check if the functions are loaded correctly
//     if (!SET || !GET || !DEL || !start_compaction) {
//         fprintf(stderr, "Failed to load functions from shared library\n");
//         exit(EXIT_FAILURE);
//     }
// }

// Parse the RESP2 protocol
int parse_resp(char *message, char **command, char **arg1, char **arg2)
{
    if (message[0] == '*')
    {
        // Split the message by "\r\n"
        char *token = strtok(message, "\r\n");

        // Ensure the message is correctly formatted and has enough parts
        if (token == NULL)
            return 0;

        // Skip the '*' marker
        token = strtok(NULL, "\r\n");
        if (token == NULL)
            return 0;

        // Get the command (SET/GET/DEL or other commands)
        *command = strtok(NULL, "\r\n");
        if (*command == NULL)
            return 0;

        // Get the first argument (key) if it exists
        char *len_key = strtok(NULL, "\r\n");
        *arg1 = strtok(NULL, "\r\n");

        char *len_value = strtok(NULL, "\r\n");
        if (len_value == NULL)
            return 1;
        // Get the second argument (value for SET) if it exists
        *arg2 = strtok(NULL, "\r\n");

        // If there are arguments and the command is valid, return success
        return 1;
    }

    // Return failure if the message does not start with '*'
    return 0;
}

// Build the RESP2 response
char *build_resp(const char *message)
{
    size_t len = strlen(message);
    char *response = (char *)malloc(len + 4); // Adding 4 for the RESP header (`+` and `\r\n`)
    sprintf(response, "+%s\r\n", message);
    return response;
}

char *build_resp_get(const char *message)
{
    size_t len = strlen(message);
    char *response = (char *)malloc(len + 6); // Adding 7 for the RESP header (`$len\r\n` and `\r\n`)
    sprintf(response, "$%zu\r\n%s\r\n", len, message);
    return response;
}

char *build_error(const char *message)
{
    size_t len = strlen(message);
    char *response = (char *)malloc(len + 8); // Adding 9 for the RESP header (`-ERR ` and `\r\n`)
    sprintf(response, "-ERR %s\r\n", message);
    return response;
}

// Handle SET command
void handle_set(int sockfd, const char *key, const char *value)
{
    SET(key, value);
    char *response = build_resp("OK");
    send_message(sockfd, response, strlen(response)); // Use send instead of write
    free(response);
}

// Handle GET command
void handle_get(int sockfd, const char *key)
{
    const char *result = GET(key);

    if (result && strcmp(result, "tombstone") != 0)
    {
        char *response = build_resp_get(result);
        send_message(sockfd, response, strlen(response)); // Use send instead of write
        free(response);
    }
    else
    {
        char *response = build_error("Key not found");
        send_message(sockfd, response, strlen(response)); // Use send instead of write
        free(response);
    }
}

// Handle DEL command
void handle_del(int sockfd, const char *key)
{
    const char *result = GET(key);
    if (result && strcmp(result, "tombstone") != 0)
    {
        DEL(key);
        char *response = build_resp("OK");
        send_message(sockfd, response, strlen(response)); // Use send instead of write
        free(response);
    }
    else
    {
        char *response = build_error("Key not found");
        send_message(sockfd, response, strlen(response)); // Use send instead of write
        free(response);
    }
}

// Set the socket to non-blocking
void set_non_blocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
    {
        perror("fcntl(F_GETFL)");
        exit(EXIT_FAILURE);
    }
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Main server loop
void handle_client(int client_fd)
{
    char buffer[MAXLINE];
    ssize_t n;

    // while (1) {
    n = receive_message(client_fd, buffer, MAXLINE); // Use recv instead of read
    if (n == 0)
    {
        close(client_fd);
        return;
    }
    // if (n == 0) {
    //     // Client closed the connection
    //     // close(client_fd);
    //     break;
    // }
    // else if (n < 0) {
    //     if (errno == EAGAIN || errno == EWOULDBLOCK) {
    //         // No data available yet, just return to the event loop
    //         break;
    //     }
    //     perror("recv");
    //     close(client_fd);
    //     break;
    // }

    buffer[n] = '\0'; // Null terminate the message
    char *command = NULL, *arg1 = NULL, *arg2 = NULL;

    if (parse_resp(buffer, &command, &arg1, &arg2))
    {
        if (strcmp(command, "SET") == 0 && arg1 && arg2)
        {
            handle_set(client_fd, arg1, arg2);
        }
        else if (strcmp(command, "GET") == 0 && arg1)
        {
            handle_get(client_fd, arg1);
        }
        else if (strcmp(command, "DEL") == 0 && arg1)
        {
            handle_del(client_fd, arg1);
        }
        else
        {
            char *response = build_error("Invalid command or arguments");
            send_message(client_fd, response, strlen(response)); // Use send instead of write
            free(response);
        }
    }
    // }
}

// Start server
void start_server(const char *host, int port)
{
    int server_fd;
    int client_fds[MAX_CLIENTS + 1];
    for (int i = 0; i < MAX_CLIENTS + 1; i++)
    {
        client_fds[i] = 0;
    }
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("ERROR opening socket");
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("ERROR on binding");
        exit(EXIT_FAILURE);
    }

    listen(server_fd, MAX_CLIENTS);
    printf("Server running on %s:%d\n", host, port);

    set_non_blocking(server_fd);

    fd_set read_fds;
    int max_fd = server_fd;
    FD_ZERO(&master_fds);
    FD_SET(server_fd, &master_fds);

    while (1)
    {
        read_fds = master_fds;
        // Add all client sockets to read_fds
        // for (int i = 3; i <= max_fd; i++) {
        //     if (i != server_fd) {
        //         FD_SET(i, &read_fds);
        //     }
        // }

        // Wait for an activity on the sockets
        int activity = select(max_fd + 1, &read_fds, NULL, NULL, NULL);

        if (activity < 0)
        {
            // Print the error number

            // Print the error message associated with the error number
            printf("Error message: %s\n", strerror(errno));
            perror("select()");
            exit(EXIT_FAILURE);
        }

        // If something happened on the server socket, it's an incoming connection
        if (FD_ISSET(server_fd, &read_fds))
        {
            int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
            if (client_fd < 0)
            {
                perror("accept()");
                continue;
            }

            // printf("New connection from %s\n", inet_ntoa(client_addr.sin_addr));

            set_non_blocking(client_fd);

            // Add new client socket to read_fds
            FD_SET(client_fd, &master_fds);
            if (client_fd > max_fd)
            {
                max_fd = client_fd;
            }
        }

        // if(FD_ISSET(STDIN_FILENO, &read_fds)){
        //     printf("ok done\n");
        // }
        // Handle data for all clients
        for (int i = 3; i <= max_fd; i++)
        {
            if (FD_ISSET(i, &read_fds))
            {
                if (i != server_fd)
                {
                    handle_client(i);
                }
            }
        }
    }

    close(server_fd);
}

int main(int argc, char *argv[])
{
    const char *host = "127.0.0.1";
    int port = PORT;

    if (argc > 1)
    {
        port = atoi(argv[1]);
    }

    // init_db(); // Initialize the database library and functions
    start_compaction();
    start_server(host, port);

    // dlclose(libdb); // Close the library after the server shuts down
    return 0;
}