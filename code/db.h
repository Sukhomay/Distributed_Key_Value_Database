#ifndef DB_H
#define DB_H

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include <stdexcept>
#include <ctime>
#include <iomanip>
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
#include <set>
#include <map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <cstdlib>
#include <fstream>
#include <sstream>

using namespace std;

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
#define MAX_STR_SIZE 1024 // Assume this as the maximum size of key or value
#define JOB_REP_SHM_NAME "/jobmanager_replica_comm"
#define HEARTBEAT_MSSG "__HEARTBEAT__"
#define MAX_EVENTS 10
// Define the only delimiter allowed for serialization.
const char DELIM = '#';
const string FIELD_DELIM = "#";
const string LIST_DELIM = "##";
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

string operationToString(Operation op)
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
        cout << "(" << availability_zone_id << ", " << slot_id << ")";
    }

    // Serialize this ReplicaID as two tokens separated by DELIM.
    // (Note: This method is for standalone use. When nesting ReplicaID into another record,
    //  output its fields directly to avoid extra delimiters.)
    string serialize() const
    {
        return to_string(availability_zone_id) + DELIM + to_string(slot_id);
    }

    // Deserialize a standalone string that is in the form: "<availability_zone_id><DELIM><slot_id>"
    static ReplicaID deserialize(const string &s)
    {
        ReplicaID replica;
        stringstream ss(s);
        string token;
        if (getline(ss, token, DELIM))
        {
            replica.availability_zone_id = stoi(token);
        }
        if (getline(ss, token, DELIM))
        {
            replica.slot_id = stoi(token);
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

typedef struct SiblingReplica
{
    vector<ReplicaID> replicas;

    // Print the SiblingReplica contents.
    void print() const
    {
        cout << "SiblingReplica (" << replicas.size() << " replicas):" << endl;
        for (const auto &replica : replicas)
        {
            cout << "  ";
            replica.print();
            cout << endl;
        }
    }

    // Serialize as a flat sequence:
    // <count><DELIM>
    // for each ReplicaID: <availability_zone_id><DELIM><slot_id><DELIM>
    string serialize() const
    {
        stringstream ss;
        ss << replicas.size();
        for (size_t i = 0; i < replicas.size(); i++)
        {
            ss << DELIM << replicas[i].availability_zone_id
               << DELIM << replicas[i].slot_id;
        }
        return ss.str();
    }

    // Deserialize from the format produced by serialize()
    static SiblingReplica deserialize(const string &s)
    {
        SiblingReplica sib;
        stringstream ss(s);
        string token;

        int count = 0;
        if (getline(ss, token, DELIM))
        {
            count = stoi(token);
        }
        for (int i = 0; i < count; i++)
        {
            ReplicaID replica;
            if (getline(ss, token, DELIM))
                replica.availability_zone_id = stoi(token);
            if (getline(ss, token, DELIM))
                replica.slot_id = stoi(token);
            sib.replicas.push_back(replica);
        }
        return sib;
    }
} SiblingReplica;

// -------------------------------------------------------------------------------
// RequestQuery with member functions

typedef struct RequestQuery
{
    ReplicaID request_replica_id;
    vector<ReplicaID> sibling_replica_id;
    long long request_id;
    Operation operation;
    int key_len;
    char key[MAX_STR_SIZE];
    int value_len;
    char value[MAX_STR_SIZE];

    void reset()
    {
        sibling_replica_id.clear();
        operation = Operation::UNKNOWN;
        key_len = value_len = 0;
    }

    // Print the RequestQuery in a decorative box.
    void print() const
    {
        string border = "+--------------------------------------------------------+";
        cout << border << "\n";
        cout << "| RequestQuery:" << "\n";
        cout << "|   Request Replica: ";
        request_replica_id.print();
        cout << "\n";
        cout << "|   Other Replicas (" << sibling_replica_id.size() << "):\n";
        for (const auto &rep : sibling_replica_id)
        {
            cout << "|      ";
            rep.print();
            cout << "\n";
        }
        cout << "|   Request ID: " << request_id << "\n";
        cout << "|   Operation: " << operationToString(operation) << "\n";
        cout << "|   Key: " << key << "\n";
        cout << "|   Value: " << value << "\n";
        cout << border << "\n";
    }

    // Serialize as a flat sequence of tokens using only DELIM:
    // <req_rep_avz><DELIM><req_rep_slot><DELIM>
    // <sibling_replicas_count><DELIM> then for each other replica: <avz><DELIM><slot><DELIM>
    // <request_id><DELIM><operation_int><DELIM><key><DELIM><value>
    string serialize() const
    {
        stringstream ss;
        // Serialize request_replica_id fields.
        ss << request_replica_id.availability_zone_id << DELIM
           << request_replica_id.slot_id << DELIM;
        // Serialize sibling_replica_id vector.
        ss << sibling_replica_id.size();
        for (size_t i = 0; i < sibling_replica_id.size(); i++)
        {
            ss << DELIM << sibling_replica_id[i].availability_zone_id
               << DELIM << sibling_replica_id[i].slot_id;
        }
        ss << DELIM << request_id;
        ss << DELIM << static_cast<int>(operation);
        ss << DELIM << key;
        ss << DELIM << value;
        return ss.str();
    }

    // Deserialize from the format produced by serialize()
    static RequestQuery deserialize(const string &s)
    {
        RequestQuery rq;
        stringstream ss(s);
        string token;

        // Deserialize request_replica_id (2 tokens).
        if (getline(ss, token, DELIM))
            rq.request_replica_id.availability_zone_id = stoi(token);
        if (getline(ss, token, DELIM))
            rq.request_replica_id.slot_id = stoi(token);

        // Deserialize sibling_replica_id vector size.
        int vectorSize = 0;
        if (getline(ss, token, DELIM))
            vectorSize = stoi(token);
        for (int i = 0; i < vectorSize; i++)
        {
            ReplicaID r;
            if (getline(ss, token, DELIM))
                r.availability_zone_id = stoi(token);
            if (getline(ss, token, DELIM))
                r.slot_id = stoi(token);
            rq.sibling_replica_id.push_back(r);
        }

        // Deserialize request_id.
        if (getline(ss, token, DELIM))
            rq.request_id = stoll(token);
        // Deserialize operation.
        if (getline(ss, token, DELIM))
            rq.operation = static_cast<Operation>(stoi(token));
        // Deserialize key.
        if (getline(ss, token, DELIM))
        {
            strncpy(rq.key, token.c_str(), MAX_STR_SIZE);
            rq.key[MAX_STR_SIZE - 1] = '\0';
            rq.key_len = token.size();
        }
        // Deserialize value (rest of string).
        if (getline(ss, token))
        {
            strncpy(rq.value, token.c_str(), MAX_STR_SIZE);
            rq.value[MAX_STR_SIZE - 1] = '\0';
            rq.value_len = token.size();
        }
        return rq;
    }
} RequestQuery;

// -------------------------------------------------------------------------------
// ReplyResponse with member functions

typedef struct ReplyResponse
{
    ReplicaID reponse_replica_id; // Field name kept as provided.
    long long request_id;
    ReturnStatus status;
    int value_len;
    char value[MAX_STR_SIZE];

    void reset()
    {
        value_len = 0;
    }

    // Print the ReplyResponse in a decorative box.
    void print() const
    {
        string border = "+--------------------------------------------------------+";
        cout << border << "\n";
        cout << "| ReplyResponse:" << "\n";
        cout << "|   Response Replica: ";
        reponse_replica_id.print();
        cout << "\n";
        cout << "|   Request ID: " << request_id << "\n";
        cout << "|   Status: " << (status == SUCCESS ? "SUCCESS" : "FAILURE") << "\n";
        cout << "|   Value: " << value << "\n";
        cout << border << "\n";
    }

    // Serialize as:
    // <response_rep_avz><DELIM><response_rep_slot><DELIM>
    // <request_id><DELIM><status_int><DELIM><value>
    string serialize() const
    {
        stringstream ss;
        ss << reponse_replica_id.availability_zone_id << DELIM
           << reponse_replica_id.slot_id << DELIM;
        ss << request_id << DELIM;
        ss << static_cast<int>(status) << DELIM;
        ss << value;
        return ss.str();
    }

    // Deserialize from the format produced by serialize()
    static ReplyResponse deserialize(const string &s)
    {
        ReplyResponse rr;
        stringstream ss(s);
        string token;

        // Deserialize reponse_replica_id (2 tokens).
        if (getline(ss, token, DELIM))
            rr.reponse_replica_id.availability_zone_id = stoi(token);
        if (getline(ss, token, DELIM))
            rr.reponse_replica_id.slot_id = stoi(token);

        // Deserialize request_id.
        if (getline(ss, token, DELIM))
            rr.request_id = stoll(token);
        // Deserialize status.
        if (getline(ss, token, DELIM))
            rr.status = static_cast<ReturnStatus>(stoi(token));
        // Deserialize value.
        if (getline(ss, token))
        {
            strncpy(rr.value, token.c_str(), MAX_STR_SIZE);
            rr.value[MAX_STR_SIZE - 1] = '\0';
            rr.value_len = token.size();
        }
        return rr;
    }
} ReplyResponse;

// -----------------------------------------------------------------------------------
typedef struct RequestToReplica
{
    sem_t sem;
    RequestQuery request;
} RequestToReplica;

typedef struct ReplyFromReplica
{
    sem_t sem;
    ReplyResponse reply;
} ReplyFromReplica;

// ------------------------------------------------------------------------------------------------------
// -----------------------------------------------------------------------------
enum Role
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

enum Message
{
    VoteRequest,
    VoteResponse,
    LogRequest,
    LogResponse
};

// -------------------------------------------------------------------------------
// LogEntry with member functions

typedef struct LogEntry
{
    int term;
    string msg;

    LogEntry(int t = 0, const string &m = "") : term(t), msg(m) {}

    // Reset to default.
    void reset()
    {
        term = 0;
        msg.clear();
    }

    // Print LogEntry nicely.
    void print() const
    {
        cout << "LogEntry(term: " << term << ", msg: " << msg << ")";
    }

    // Serialize as: <term><DELIM><msg>
    string serialize() const
    {
        return to_string(term) + DELIM + msg;
    }

    // Deserialize a LogEntry.
    static LogEntry deserialize(const string &s)
    {
        LogEntry le;
        stringstream ss(s);
        string token;
        if (getline(ss, token, DELIM))
            le.term = stoi(token);
        if (getline(ss, token))
            le.msg = token;
        return le;
    }
} LogEntry;

// -------------------------------------------------------------------------------
// RaftQuery with member functions

typedef struct RaftQuery
{
    bool valid;
    Message msg_type;
    ReplicaID sender;
    int currentTerm;
    int lastTerm;
    int prefixTerm;
    int prefixLen;
    int commitLength;
    int logLength;
    bool granted;
    vector<LogEntry> suffix;
    int ack;
    bool success;

    // Reset all fields.
    void reset()
    {
        valid = false;
        msg_type = VoteRequest;
        currentTerm = 0;
        lastTerm = 0;
        prefixTerm = 0;
        prefixLen = 0;
        commitLength = 0;
        logLength = 0;
        granted = false;
        suffix.clear();
        ack = 0;
        success = false;
    }

    // Print RaftQuery inside a decorative box.
    void print() const
    {
        string border = "+--------------------------------------------------------+";
        cout << border << "\n";
        cout << "| RaftQuery:\n";
        cout << "|   Valid: " << (valid ? "true" : "false") << "\n";
        cout << "|   Message Type: " << static_cast<int>(msg_type) << "\n";
        cout << "|   Sender: ";
        sender.print();
        cout << "\n";
        cout << "|   Current Term: " << currentTerm << "\n";
        cout << "|   Last Term: " << lastTerm << "\n";
        cout << "|   Prefix Term: " << prefixTerm << "\n";
        cout << "|   Prefix Len: " << prefixLen << "\n";
        cout << "|   Commit Length: " << commitLength << "\n";
        cout << "|   Log Length: " << logLength << "\n";
        cout << "|   Granted: " << (granted ? "true" : "false") << "\n";
        cout << "|   Suffix Count: " << suffix.size() << "\n";
        for (const auto &le : suffix)
        {
            cout << "|     ";
            le.print();
            cout << "\n";
        }
        cout << "|   Ack: " << ack << "\n";
        cout << "|   Success: " << (success ? "true" : "false") << "\n";
        cout << border << "\n";
    }

    // Serialize RaftQuery using only DELIM.
    string serialize() const
    {
        stringstream ss;
        ss << (valid ? 1 : 0) << DELIM;
        ss << static_cast<int>(msg_type) << DELIM;
        // Sender (inline: two tokens)
        ss << sender.availability_zone_id << DELIM;
        ss << sender.slot_id << DELIM;
        ss << currentTerm << DELIM;
        ss << lastTerm << DELIM;
        ss << prefixTerm << DELIM;
        ss << prefixLen << DELIM;
        ss << commitLength << DELIM;
        ss << logLength << DELIM;
        ss << (granted ? 1 : 0) << DELIM;
        // Suffix: first output count, then for each LogEntry output its two fields.
        ss << suffix.size();
        for (const auto &le : suffix)
        {
            ss << DELIM << le.term << DELIM << le.msg;
        }
        ss << DELIM << ack << DELIM << (success ? 1 : 0);
        return ss.str();
    }

    // Deserialize from a string produced by serialize().
    static RaftQuery deserialize(const string &s)
    {
        RaftQuery rq;
        rq.reset();
        stringstream ss(s);
        string token;

        if (getline(ss, token, DELIM))
            rq.valid = (stoi(token) != 0);
        if (getline(ss, token, DELIM))
            rq.msg_type = static_cast<Message>(stoi(token));
        if (getline(ss, token, DELIM))
            rq.sender.availability_zone_id = stoi(token);
        if (getline(ss, token, DELIM))
            rq.sender.slot_id = stoi(token);
        if (getline(ss, token, DELIM))
            rq.currentTerm = stoi(token);
        if (getline(ss, token, DELIM))
            rq.lastTerm = stoi(token);
        if (getline(ss, token, DELIM))
            rq.prefixTerm = stoi(token);
        if (getline(ss, token, DELIM))
            rq.prefixLen = stoi(token);
        if (getline(ss, token, DELIM))
            rq.commitLength = stoi(token);
        if (getline(ss, token, DELIM))
            rq.logLength = stoi(token);
        if (getline(ss, token, DELIM))
            rq.granted = (stoi(token) != 0);

        int suffixCount = 0;
        if (getline(ss, token, DELIM))
            suffixCount = stoi(token);
        for (int i = 0; i < suffixCount; i++)
        {
            int le_term = 0;
            string le_msg;
            if (getline(ss, token, DELIM))
                le_term = stoi(token);
            if (getline(ss, token, DELIM))
                le_msg = token;
            rq.suffix.push_back(LogEntry(le_term, le_msg));
        }
        if (getline(ss, token, DELIM))
            rq.ack = stoi(token);
        if (getline(ss, token))
            rq.success = (stoi(token) != 0);
        return rq;
    }
} RaftQuery;

// -------------------------------------------------------------------------------
// LogMessage with member functions

typedef struct LogMessage
{
    Operation op;
    string key;
    string value;
    ReplicaID replicaID;
    string localTime;

    // Reset all fields.
    void reset()
    {
        op = GET; // Default operation
        key.clear();
        value.clear();
        localTime.clear();
    }

    // Print LogMessage in a decorative box.
    void print() const
    {
        string border = "+--------------------------------------------------------+";
        cout << border << "\n";
        cout << "| LogMessage:\n";
        cout << "|   Local Time: " << localTime << "\n";
        cout << "|   Operation: " << operationToString(op) << "\n";
        cout << "|   Key: " << key << "\n";
        cout << "|   Value: " << value << "\n";
        cout << "|   Replica: ";
        replicaID.print();
        cout << "\n"
             << border << "\n";
    }

    // Serialize as:
    // <op><DELIM><key><DELIM><value><DELIM>
    // <replicaID.availability_zone_id><DELIM><replicaID.slot_id><DELIM><localTime>
    string serialize() const
    {
        stringstream ss;
        ss << static_cast<int>(op) << DELIM;
        ss << key << DELIM;
        ss << value << DELIM;
        ss << replicaID.availability_zone_id << DELIM;
        ss << replicaID.slot_id << DELIM;
        ss << localTime;
        return ss.str();
    }

    // Deserialize from a string produced by serialize().
    static LogMessage deserialize(const string &s)
    {
        LogMessage lm;
        stringstream ss(s);
        string token;
        if (getline(ss, token, DELIM))
            lm.op = static_cast<Operation>(stoi(token));
        if (getline(ss, token, DELIM))
            lm.key = token;
        if (getline(ss, token, DELIM))
            lm.value = token;
        if (getline(ss, token, DELIM))
            lm.replicaID.availability_zone_id = stoi(token);
        if (getline(ss, token, DELIM))
            lm.replicaID.slot_id = stoi(token);
        if (getline(ss, token))
            lm.localTime = token;
        return lm;
    }

    // Format LogMessage to a nicely formatted string.
    string format() const
    {
        ostringstream oss;
        oss << "[" << localTime << "] "
            << "Operation: " << operationToString(op) << ", "
            << "Key: " << key << ", "
            << "Value: " << value << ", "
            << "Replica: (" << replicaID.availability_zone_id << ", "
            << replicaID.slot_id << ")";
        return oss.str();
    }
} LogMessage;

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
#include "json.hpp"
using json = nlohmann::json;
pair<string, int> getReplicaAddr(const ReplicaID &replica)
{
    int az_id = replica.availability_zone_id;
    // Load JSON from file. Adjust the filename as needed.
    ifstream inFile("CONFIG.json");
    if (!inFile)
    {
        cerr << "Unable to open file\n";
        return make_pair("", 0);
    }

    json j;
    inFile >> j;
    inFile.close();

    pair<string, int> addr = make_pair("", 0);

    // Iterate over the nodes array in the JSON object.
    for (const auto &node : j["nodes"])
    {
        // Check if the node is an availability zone and has the requested id.
        if (node.contains("type") && node["type"] == "availability_zone" &&
            node.contains("id") && std::stoi(node["id"].get<std::string>()) == az_id)
        {
            // Extract host and port from the node.
            string host = node.value("host", "");
            int port = stoi(node.value("port", ""));
            addr = make_pair(host, port);
            break;
        }
    }

    if (addr.first.empty())
    {
        cout << "Availability zone with id " << az_id << " not found." << endl;
    }

    return addr;
}

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------

// Helper function to get the current local time as a string in "YYYY-MM-DD HH:MM:SS" format.
string getCurrentLocalTime()
{
    auto now = chrono::system_clock::now();
    time_t now_time = chrono::system_clock::to_time_t(now);
    struct tm local_tm;
#if defined(_WIN32) || defined(_WIN64)
    localtime_s(&local_tm, &now_time);
#else
    localtime_r(&now_time, &local_tm);
#endif
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &local_tm);
    return string(buffer);
}

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
    char buffer[512];
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
struct Address
{
    string host;
    int port;

    void reset()
    {
        host.clear();
        port = 0;
    }

    // Print Address in the form: host:port
    void print() const
    {
        cout << host << ":" << port;
    }

    // Serialize as: <host><DELIM><port>
    string serialize() const
    {
        return host + DELIM + to_string(port);
    }

    // Deserialize from a stream (reads two tokens).
    static Address deserializeFromStream(istream &is)
    {
        Address addr;
        string token;
        getline(is, token, DELIM);
        addr.host = token;
        getline(is, token, DELIM);
        addr.port = stoi(token);
        return addr;
    }
};

// -------------------------------------------------------------------------------
// ReplicaInfo with member functions
struct ReplicaInfo
{
    // Map from ReplicaID to Address.
    map<ReplicaID, Address> replicas;

    // Print all pairs in a friendly format.
    void print() const
    {
        cout << "ReplicaInfo:" << endl;
        for (const auto &entry : replicas)
        {
            cout << "  ";
            entry.first.print();
            cout << " -> ";
            entry.second.print();
            cout << endl;
        }
    }

    // Serialize the ReplicaInfo using only DELIM.
    // Format: <N><DELIM>[for each entry: <az_id><DELIM><slot_id><DELIM><host><DELIM><port>]
    string serialize() const
    {
        ostringstream oss;
        oss << replicas.size();
        // For each replica in the map, output exactly four tokens.
        for (const auto &entry : replicas)
        {
            oss << DELIM
                << entry.first.availability_zone_id << DELIM
                << entry.first.slot_id << DELIM
                << entry.second.host << DELIM
                << entry.second.port;
        }
        return oss.str();
    }

    // Deserialize from a string using only DELIM.
    // Must match the format as defined above.
    static ReplicaInfo deserialize(const string &s)
    {
        ReplicaInfo info;
        istringstream iss(s);
        string token;

        // Read count.
        int count = 0;
        if (getline(iss, token, DELIM))
            count = stoi(token);

        // Loop for each entry. For each, read 4 tokens.
        for (int i = 0; i < count; i++)
        {
            ReplicaID rid;
            Address addr;

            // Read ReplicaID tokens.
            if (getline(iss, token, DELIM))
                rid.availability_zone_id = stoi(token);
            if (getline(iss, token, DELIM))
                rid.slot_id = stoi(token);
            // Read Address tokens.
            if (getline(iss, token, DELIM))
                addr.host = token;
            if (getline(iss, token, DELIM))
                addr.port = stoi(token);

            info.replicas.insert({rid, addr});
        }
        return info;
    }
};

// ------------------------------------------------------------------------------------------------------

#endif // DB_H
