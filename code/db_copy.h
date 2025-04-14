#ifndef DB_H
#define DB_H

// File and system I/O.
#include <fcntl.h>     // For open(), O_ flags, etc.
#include <unistd.h>    // For lseek(), write(), read(), ftruncate(), close(), etc.
#include <sys/types.h> // For types like ssize_t.
#include <sys/stat.h>  // For file modes (S_IRUSR, S_IWUSR, etc.)
#include <sys/mman.h>  // For memory mapping functions.
#include <sys/shm.h>   // For shared memory functions.

// Network related headers.
#include <sys/socket.h> // For socket APIs.
#include <netinet/in.h> // For sockaddr_in, etc.
#include <arpa/inet.h>  // For inet_pton(), etc.

// Threading and synchronization.
#include <thread>
#include <mutex>
#include <semaphore.h>
#include <chrono>

// Standard C and C++ libraries.
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <set>
#include <map>
#include <utility>
#include <algorithm>
#include <stdexcept>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <ctime>
#include <iomanip>
#include <sys/epoll.h>
#include <sys/wait.h>

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
#define MAX_STR_SIZE 1024 // Assume this as the maximum size of key or value
#define HEARTBEAT_MSSG "__HEARTBEAT__"
#define MAX_EVENTS 10
// Define the only delimiter allowed for serialization.
const char DELIM = '#';
const std::string FIELD_DELIM = "#";
const std::string LIST_DELIM = "##";
#define nullValue -1
#define nullReplica ReplicaID{-1, -1}

// ------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------
enum ReturnStatus
{
    FAILURE = 0,
    SUCCESS = 1
};

enum Operation
{
    CREATE_PROPAGATE, // The first create send to a job manager who is responsible for sending to other job managers to create the respective replicas
    CREATE,
    GET,
    SET,
    DEL,
    UNKNOWN
};

std::string operationToString(Operation op)
{
    switch (op)
    {
    case GET:
        return "GET";
    case SET:
        return "SET";
    case DEL:
        return "DEL";
    case CREATE:
        return "CREATE";
    case CREATE_PROPAGATE:
        return "CREATE_PROPAGATE";
    default:
        return "UNKNOWN";
    }
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// ReplicaID with member functions

typedef struct ReplicaID
{
    int availability_zone_id;
    int slot_id;

    // Print the ReplicaID (one-line output)
    void print() const
    {
        std::cout << "(" << availability_zone_id << ", " << slot_id << ")";
    }

    // Serialize this ReplicaID as two tokens separated by DELIM.
    // (Note: This method is for standalone use. When nesting ReplicaID into another record,
    //  output its fields directly to avoid extra delimiters.)
    std::string serialize() const
    {
        return std::to_string(availability_zone_id) + DELIM + std::to_string(slot_id);
    }

    // Deserialize a standalone string that is in the form: "<availability_zone_id><DELIM><slot_id>"
    static ReplicaID deserialize(const std::string &s)
    {
        ReplicaID replica;
        std::stringstream ss(s);
        std::string token;
        if (std::getline(ss, token, DELIM))
        {
            replica.availability_zone_id = std::stoi(token);
        }
        if (std::getline(ss, token, DELIM))
        {
            replica.slot_id = std::stoi(token);
        }
        return replica;
    }

    bool operator==(const ReplicaID &replica) const
    {
        return (availability_zone_id == replica.availability_zone_id &&
                slot_id == replica.slot_id);
    }

    bool operator<(const ReplicaID &other) const
    {
        return (availability_zone_id < other.availability_zone_id) ||
               (availability_zone_id == other.availability_zone_id && slot_id < other.slot_id);
    }

    bool operator!=(const ReplicaID &other) const
    {
        return !(*this == other);
    }
} ReplicaID;

// -------------------------------------------------------------------------------
// SiblingReplica contains a vector of ReplicaID and corresponding member functions.

// -------------------------------------------------------------------------------
// RequestQuery with member functions

// -------------------------------------------------------------------------------
typedef struct RequestQuery
{
    ReplicaID request_replica_id;
    std::vector<ReplicaID> sibling_replica_id;
    long long request_id;
    Operation operation;
    std::string key;
    std::string value;

    // --- Updated nice printing ---
    void print() const
    {
        std::cout << "----------------" << std::endl;  // Begin separator

        std::cout << "RequestQuery:" << std::endl;
        std::cout << "  Request Replica ID: ";
        request_replica_id.print();
        std::cout << std::endl;

        std::cout << "  Sibling Replica IDs: [";
        for (size_t i = 0; i < sibling_replica_id.size(); ++i)
        {
            sibling_replica_id[i].print();
            if (i != sibling_replica_id.size() - 1)
                std::cout << ", ";
        }
        std::cout << "]" << std::endl;

        std::cout << "  Request ID: " << request_id << std::endl;
        std::cout << "  Operation: " << operationToString(operation) << std::endl;
        std::cout << "  Key: " << key << std::endl;
        std::cout << "  Value: " << value << std::endl;

        std::cout << "----------------" << std::endl;  // End separator
    }

    // --- (Existing serialize() and deserialize() methods remain unchanged) ---
    std::string serialize() const
    {
        const char DELIM = '#';
        std::string serialized;
        
        // Serialize the request replica ID.
        serialized += request_replica_id.serialize() + DELIM;
        
        // Serialize the sibling replica IDs: first the number of siblings.
        serialized += std::to_string(sibling_replica_id.size()) + DELIM;
        // Then serialize each sibling replica.
        for (size_t i = 0; i < sibling_replica_id.size(); ++i)
        {
            serialized += sibling_replica_id[i].serialize();
            if (i != sibling_replica_id.size() - 1)
                serialized += DELIM;
        }
        serialized += DELIM;
        
        // Serialize request ID, operation, key, and value.
        serialized += std::to_string(request_id) + DELIM;
        serialized += operationToString(operation) + DELIM;
        serialized += key + DELIM;
        serialized += value; // No trailing delimiter needed.
        
        return serialized;
    }

    // Deserializes a string into a RequestQuery.
    static RequestQuery deserialize(const std::string &s)
    {
        RequestQuery rq;
        const char DELIM = '#';
        std::istringstream iss(s);
        std::string token;

        // Deserialize request_replica_id from its two tokens.
        if (std::getline(iss, token, DELIM))
        {
            rq.request_replica_id.availability_zone_id = std::stoi(token);
        }
        if (std::getline(iss, token, DELIM))
        {
            rq.request_replica_id.slot_id = std::stoi(token);
        }
        
        // Next token is the number of sibling replicas.
        size_t siblingCount = 0;
        if (std::getline(iss, token, DELIM))
        {
            siblingCount = std::stoul(token);
        }
        rq.sibling_replica_id.resize(siblingCount);
        
        // Deserialize each sibling ReplicaID (each consists of two tokens).
        for (size_t i = 0; i < siblingCount; ++i)
        {
            ReplicaID rep;
            if (std::getline(iss, token, DELIM))
            {
                rep.availability_zone_id = std::stoi(token);
            }
            if (std::getline(iss, token, DELIM))
            {
                rep.slot_id = std::stoi(token);
            }
            rq.sibling_replica_id[i] = rep;
        }
        
        // Next token: request_id.
        if (std::getline(iss, token, DELIM))
        {
            rq.request_id = std::stoll(token);
        }
        
        // Next token: operation as a string.
        std::string opStr;
        if (std::getline(iss, opStr, DELIM))
        {
            if (opStr == "GET")
                rq.operation = GET;
            else if (opStr == "SET")
                rq.operation = SET;
            else if (opStr == "DEL")
                rq.operation = DEL;
            else if (opStr == "CREATE")
                rq.operation = CREATE;
            else if (opStr == "CREATE_PROPAGATE")
                rq.operation = CREATE_PROPAGATE;
            else
                rq.operation = UNKNOWN;
        }
        
        // Next token: key.
        if (std::getline(iss, token, DELIM))
        {
            rq.key = token;
        }
        // The rest of the string is the value.
        if (std::getline(iss, token))
        {
            rq.value = token;
        }
        
        return rq;
    }
}RequestQuery;

// -------------------------------------------------------------------------------
typedef struct ReplyResponse
{
    ReplicaID reponse_replica_id;  // Field name kept as provided.
    long long request_id;
    ReturnStatus status;
    std::string value;

    // Helper: Convert the ReturnStatus enum to a string representation.
    std::string statusToString() const
    {
        return (status == SUCCESS) ? "SUCCESS" : "FAILURE";
    }

    // Print function that includes dashed separators before and after printing the record.
    void print() const
    {
        std::cout << "----------------" << std::endl;
        std::cout << "ReplyResponse:" << std::endl;
        std::cout << "  Replica ID: ";
        reponse_replica_id.print();
        std::cout << std::endl;
        std::cout << "  Request ID: " << request_id << std::endl;
        std::cout << "  Status: " << statusToString() << std::endl;
        std::cout << "  Value: " << value << std::endl;
        std::cout << "----------------" << std::endl;
    }

    // Serialization function: constructs a string separated by '#' delimiters.
    // The format will be:
    // <replica_avail>#<replica_slot>#<request_id>#<status_as_string>#<value>
    std::string serialize() const
    {
        const char DELIM = '#';
        std::string serialized;
        serialized += reponse_replica_id.serialize() + DELIM;
        serialized += std::to_string(request_id) + DELIM;
        serialized += statusToString() + DELIM;
        serialized += value; // No trailing delimiter.
        return serialized;
    }

    // Deserialization function: parses a string (created by serialize()) back into a ReplyResponse.
    static ReplyResponse deserialize(const std::string &s)
    {
        ReplyResponse rr;
        const char DELIM = '#';
        std::istringstream iss(s);
        std::string token;

        // Deserialize the ReplicaID, which consists of two tokens.
        if (std::getline(iss, token, DELIM))
            rr.reponse_replica_id.availability_zone_id = std::stoi(token);
        if (std::getline(iss, token, DELIM))
            rr.reponse_replica_id.slot_id = std::stoi(token);

        // Next token: request_id.
        if (std::getline(iss, token, DELIM))
            rr.request_id = std::stoll(token);

        // Next token: status as a string. Convert to ReturnStatus enum.
        std::string statusStr;
        if (std::getline(iss, statusStr, DELIM))
        {
            if (statusStr == "SUCCESS")
                rr.status = SUCCESS;
            else
                rr.status = FAILURE;
        }

        // The rest of the line is the value.
        if (std::getline(iss, token))
            rr.value = token;

        return rr;
    }
}ReplyResponse;
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------

// Custom send_all function that sends the entire message.
bool send_all(int sockfd, std::string message, char delimiter = '\n')
{
    message.push_back(delimiter);
    size_t total_sent = 0;
    size_t message_len = message.size();
    while (total_sent < message_len)
    {
        int sent = send(sockfd, message.data() + total_sent, message_len - total_sent, 0);
        if (sent < 0)
        {
            if (errno == EINTR)
            {
                continue; // Retry if interrupted by a signal.
            }
            perror("send");
            message.pop_back();
            return false;
        }
        else if (sent == 0)
        {
            // Connection closed unexpectedly.
            break;
        }
        total_sent += sent;
    }
    message.pop_back();
    return total_sent == message_len;
}

// Custom function to receive data until a delimiter is found.
// The result is stored in the provided string reference.
// Returns true if the delimiter was found and data received successfully, false otherwise.
bool recv_all(int sockfd, std::string &result, char delimiter = '\n')
{
    result.clear();
    char buffer[MAX_STR_SIZE];
    while (true)
    {
        int recvd = recv(sockfd, buffer, sizeof(buffer), 0);
        if (recvd < 0)
        {
            if (errno == EINTR)
                continue; // Retry if interrupted by a signal.
            perror("recv");
            return false;
        }
        else if (recvd == 0)
        {
            // Connection closed before delimiter was found.
            std::cerr << "Connection closed before receiving the complete message." << std::endl;
            return false;
        }
        result.append(buffer, recvd);
        // If the delimiter is found, stop reading.
        if (result.find(delimiter) != std::string::npos)
        {
            result.pop_back();
            break;
        }
    }
    return true;
}
// ------------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// Address with member functions
typedef struct Address
{
    std::string host;
    int port;

    void reset()
    {
        host.clear();
        port = 0;
    }

    // Print Address in the form: host:port
    void print() const
    {
        std::cout << host << ":" << port;
    }

    // Serialize as: <host><DELIM><port>
    std::string serialize() const
    {
        return host + DELIM + std::to_string(port);
    }

    static Address deserialize(const std::string &data)
    {
        Address addr;
        size_t delim_pos = data.find(DELIM);
        if (delim_pos == std::string::npos)
            throw std::invalid_argument("Invalid format: missing delimiter");

        addr.host = data.substr(0, delim_pos);
        std::string port_str = data.substr(delim_pos + 1);

        try
        {
            addr.port = std::stoi(port_str);
        }
        catch (const std::exception &e)
        {
            throw std::invalid_argument("Invalid port number");
        }

        return addr;
    }
} Address;

std::string serializeReplicaAddrMap(const std::map<ReplicaID, Address> &replica_map)
{
    std::string result;
    for (const auto &entry : replica_map)
    {
        const ReplicaID &rid = entry.first;
        const Address &addr = entry.second;

        result += std::to_string(rid.availability_zone_id) + DELIM +
                  std::to_string(rid.slot_id) + DELIM +
                  addr.host + DELIM +
                  std::to_string(addr.port) + DELIM;
    }

    if (!result.empty())
        result.pop_back(); // Remove trailing DELIM

    return result;
}

std::map<ReplicaID, Address> deserializeReplicaAddrMap(const std::string &data)
{
    std::map<ReplicaID, Address> replica_map;
    std::stringstream ss(data);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, DELIM))
    {
        tokens.push_back(token);
    }

    if (tokens.size() % 4 != 0)
    {
        throw std::invalid_argument("Malformed input for map deserialization");
    }

    for (size_t i = 0; i < tokens.size(); i += 4)
    {
        ReplicaID rid;
        Address addr;

        rid.availability_zone_id = std::stoi(tokens[i]);
        rid.slot_id = std::stoi(tokens[i + 1]);
        addr.host = tokens[i + 2];
        addr.port = std::stoi(tokens[i + 3]);

        replica_map[rid] = addr;
    }

    return replica_map;
}



// ------------------------------------------------------------------------------------------------------
#include "json.hpp"
using json = nlohmann::json;
Address getReplicaAddr(const ReplicaID &replica)
{
    int az_id = replica.availability_zone_id;
    // Load JSON from file. Adjust the filename as needed.
    std::ifstream inFile("CONFIG.json");
    if (!inFile)
    {
        std::cerr << "Unable to open file" << std::endl;
        return Address(); // Return a default Address with empty host and port 0.
    }

    json j;
    inFile >> j;
    inFile.close();

    Address addr; // Default: host is empty, port is 0.

    // Iterate over the nodes array in the JSON object.
    for (const auto &node : j["nodes"])
    {
        // Check if the node is an availability zone and has the requested id.
        if (node.contains("type") && node["type"] == "availability_zone" &&
            node.contains("id") && std::stoi(node["id"].get<std::string>()) == az_id)
        {
            // Extract host and port from the node and store them in our Address struct.
            addr.host = node.value("host", "");
            addr.port = std::stoi(node.value("port", ""));
            break;
        }
    }

    if (addr.host.empty())
    {
        std::cout << "Availability zone with id " << az_id << " not found." << std::endl;
    }

    return addr;
}

// -------------------------------------------------------------------------------

int extractPort(const std::string &port_msg)
{
    const std::string prefix = "CONNECT TO PORT ";
    size_t pos = port_msg.find(prefix);
    if (pos == std::string::npos)
    {
        throw std::runtime_error("Invalid port message: prefix not found");
    }
    // Calculate the starting position of the port number.
    size_t start = pos + prefix.length();
    // Find the end of the port number (assume newline as delimiter).
    size_t end = port_msg.find('\n', start);
    if (end == std::string::npos)
    {
        end = port_msg.length(); // If no newline is found, take the rest of the string.
    }
    // Extract the port number as a substring.
    std::string port_str = port_msg.substr(start, end - start);
    // Convert the substring to an integer.
    int port = std::stoi(port_str);
    return port;
}

std::pair<ReplyResponse, int> sendInitialRequest(Address &addr, RequestQuery &request)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ReplyResponse fail_response;
    fail_response.status = ReturnStatus::FAILURE;
    if (sockfd < 0)
    {
        perror("socket creation failed");
        return std::make_pair(fail_response, -1);
    }
    struct sockaddr_in serv_addr;
    std::memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(addr.port);
    if (inet_pton(AF_INET, addr.host.c_str(), &serv_addr.sin_addr) <= 0)
    {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        close(sockfd);
        return std::make_pair(fail_response, -1);
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("Connection Failed");
        close(sockfd);
        return std::make_pair(fail_response, -1);
    }

    std::string resp;
    recv_all(sockfd, resp);

    std::cout << resp << std::endl;

    send_all(sockfd, "OK");

    close(sockfd);

    int new_port = extractPort(resp);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    serv_addr.sin_port = htons(new_port);
    std::memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(new_port);
    if (inet_pton(AF_INET, addr.host.c_str(), &serv_addr.sin_addr) <= 0)
    {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        close(sockfd);
        return std::make_pair(fail_response, -1);
    }

    while (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        /* code */
    }

    recv_all(sockfd, resp);
    std::cout << resp << std::endl;

    request.print();
    send_all(sockfd, request.serialize());

    recv_all(sockfd, resp);
    ReplyResponse reply = ReplyResponse::deserialize(resp);
    reply.print();

    return std::make_pair(reply, sockfd);
}

// ------------------------------------------------------------------------------------------------------

/**
 * Writes the provided string as the first line of the file referred by the file descriptor.
 *
 * @param fd   An open file descriptor.
 * @param line The string to write as the first line. A newline will be appended
 *             if the string does not end with one.
 * @return     true on success, false on failure.
 */
bool write_first_line(int fd, const std::string &line)
{
    // Reposition the file offset to the beginning of the file.
    if (lseek(fd, 0, SEEK_SET) < 0)
    {
        perror("lseek");
        return false;
    }

    // Ensure the line ends with a newline character.
    std::string output = line;
    if (output.empty() || output.back() != '\n')
    {
        output.push_back('\n');
    }

    // Write the prepared string to the file.
    ssize_t bytesWritten = write(fd, output.c_str(), output.size());
    if (bytesWritten != static_cast<ssize_t>(output.size()))
    {
        perror("write");
        return false;
    }

    // Truncate the file to remove any leftover data beyond what we just wrote.
    if (ftruncate(fd, bytesWritten) < 0)
    {
        perror("ftruncate");
        return false;
    }

    return true;
}

/**
 * Reads and returns the first line from the file referred by the file descriptor.
 *
 * @param fd An open file descriptor.
 * @return   A std::string containing the first line of the file (without the trailing newline).
 */
std::string read_first_line(int fd)
{
    // Reposition the file offset to the beginning of the file.
    if (lseek(fd, 0, SEEK_SET) < 0)
    {
        perror("lseek");
        return "";
    }

    std::string firstLine;
    char ch;

    // Read one character at a time until newline or end-of-file.
    while (true)
    {
        ssize_t bytesRead = read(fd, &ch, 1);

        if (bytesRead < 0)
        {
            // An error occurred.
            perror("read");
            return "";
        }
        else if (bytesRead == 0)
        {
            // End-of-file reached.
            break;
        }

        if (ch == '\n')
        {
            // Newline found: Stop reading further.
            break;
        }
        firstLine.push_back(ch);
    }

    return firstLine;
}

#endif // DB_H
