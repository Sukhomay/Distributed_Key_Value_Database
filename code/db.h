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

#define MAX_STR_SIZE 1024 // Assume this as the maximum size of key or value
#define JOB_REP_SHM_NAME "/jobmanager_replica_comm"
#define MAX_EVENTS 10
const string FIELD_DELIM = "#";
const string LIST_DELIM = "##";
#define nullValue -1
#define nullReplica ReplicaID{-1, -1}

enum ReturnStatus
{
    FAILURE = 0,
    SUCCESS = 1
};

enum Role
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

enum Operation
{
    CREATE_PROPAGATE, // The first create send to a job manager who is responsible for sending to other job managers to create the respective replicas
    CREATE,
    GET,
    SET,
    DEL
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
    // case RAFT:
    //     return "RAFT";
    default:
        return "UNKNOWN";
    }
}
enum Message
{
    VoteRequest,
    VoteResponse,
    LogRequest,
    LogResponse
};

// -------------------------------------------------------------------------------
typedef struct ReplicaID
{
    int availability_zone_id;
    int slot_id;
    friend istream &operator>>(istream &is, ReplicaID &replica)
    {
        is >> replica.availability_zone_id >> replica.slot_id;
        return is;
    }
    friend ostream &operator<<(ostream &os, const ReplicaID &replica)
    {
        os << "ReplicaID : " << replica.availability_zone_id << " " << replica.slot_id << endl;
        return os;
    }
    bool operator==(const ReplicaID &replica) const
    {
        return (availability_zone_id == replica.availability_zone_id && slot_id == replica.slot_id);
    }
    bool operator<(const ReplicaID &other) const
    {
        return (availability_zone_id < other.availability_zone_id) || (availability_zone_id == other.availability_zone_id && slot_id < other.slot_id);
    }
    bool operator!=(const ReplicaID &other) const
    {
        return (availability_zone_id != other.availability_zone_id || slot_id != other.slot_id);
    }
} ReplicaID;

typedef struct LogEntry
{
    int term;
    string msg;
    LogEntry(int t = 0, const string &m = "") : term(t), msg(m) {}
} LogEntry;

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

    void reset()
    {
        valid = false;
        suffix.clear();
    }
} RaftQuery;

typedef struct RequestQuery
{
    ReplicaID request_replica_id;
    vector<ReplicaID> other_replica_id;
    long long request_id;
    Operation operation;
    int key_len;
    char key[MAX_STR_SIZE];
    int value_len;
    char value[MAX_STR_SIZE];
} RequestQuery;

typedef struct ReplyResponse
{
    ReplicaID reponse_replica_id;
    long long request_id;
    ReturnStatus status;
    int value_len;
    char value[MAX_STR_SIZE];
} ReplyResponse;

// ============================================================
// Escape/deserialize functions for strings using only '#' and '@'
// ============================================================
string serializeString(const string &input)
{
    string output;
    for (char c : input)
    {
        if (c == '@')
        {
            output += "@@";
        }
        else if (c == '#')
        {
            output += "@#";
        }
        else
        {
            output.push_back(c);
        }
    }
    return output;
}

string deserializeString(const string &input)
{
    string output;
    for (size_t i = 0; i < input.size(); i++)
    {
        if (input[i] == '@' && i + 1 < input.size())
        {
            char next = input[i + 1];
            if (next == '@')
            {
                output.push_back('@');
                i++;
            }
            else if (next == '#')
            {
                output.push_back('#');
                i++;
            }
            else
            {
                output.push_back('@'); // unknown serialize: output '@'
            }
        }
        else
        {
            output.push_back(input[i]);
        }
    }
    return output;
}

// ------------------------------------------------------------
// Utility: simple split on a character delimiter (no serialize handling)
// ------------------------------------------------------------
vector<string> splitString(const string &s, char delimiter)
{
    vector<string> tokens;
    string token;
    for (char c : s)
    {
        if (c == delimiter)
        {
            tokens.push_back(token);
            token.clear();
        }
        else
        {
            token.push_back(c);
        }
    }
    tokens.push_back(token);
    return tokens;
}

// ============================================================
// ReplicaID
// ------------------------------------------------------------
// Serialize using '@' to separate its two integer fields.
string serializeReplicaID(const ReplicaID &replica)
{
    return to_string(replica.availability_zone_id) + "@" + to_string(replica.slot_id);
}

ReplicaID deserializeReplicaID(const string &data)
{
    ReplicaID r;
    vector<string> parts = splitString(data, '@');
    if (parts.size() >= 2)
    {
        r.availability_zone_id = stoi(parts[0]);
        r.slot_id = stoi(parts[1]);
    }
    return r;
}

void printReplicaID(const ReplicaID &replica)
{
    cout << "{ availability_zone_id: " << replica.availability_zone_id
         << ", slot_id: " << replica.slot_id << " }";
}

// ============================================================
// ReplicaID Vector
// ------------------------------------------------------------
// Serialize a vector of ReplicaID objects into the format:
// <vector_size>#<serialized_replicaID>#<serialized_replicaID>...
string serializeReplicaIDVector(const vector<ReplicaID> &replicas)
{
    stringstream ss;
    ss << replicas.size();
    for (size_t i = 0; i < replicas.size(); i++)
    {
        ss << "#" << serializeReplicaID(replicas[i]);
    }
    return ss.str();
}

// Deserialize a string into a vector<ReplicaID> assuming the format above.
// This function uses '#' as a delimiter.
vector<ReplicaID> deserializeReplicaIDVector(const string &data)
{
    vector<ReplicaID> replicas;
    istringstream iss(data);
    string token;

    // Retrieve the first token, which should be the vector size
    if (getline(iss, token, '#'))
    {
        int count = stoi(token);
        for (int i = 0; i < count; i++)
        {
            if (getline(iss, token, '#'))
            {
                ReplicaID replica = deserializeReplicaID(token);
                replicas.push_back(replica);
            }
            else
            {
                // Tokenizing error: found fewer elements than expected.
                break;
            }
        }
    }
    return replicas;
}
// ============================================================
// LogEntry
// ------------------------------------------------------------
// Fields: term (int) and msg (string). We use '#' as the field delimiter,
// but we serialize the string field.
string serializeLogEntry(const LogEntry &entry)
{
    return to_string(entry.term) + "#" + serializeString(entry.msg);
}

LogEntry deserializeLogEntry(const string &data)
{
    vector<string> parts = splitString(data, '#');
    int term = 0;
    string msg;
    if (parts.size() >= 2)
    {
        term = stoi(parts[0]);
        msg = deserializeString(parts[1]);
    }
    return LogEntry(term, msg);
}

void printLogEntry(const LogEntry &entry)
{
    cout << "{ term: " << entry.term << ", msg: " << entry.msg << " }";
}

// ============================================================
// RaftQuery
// ------------------------------------------------------------
// We serialize the RaftQuery as a sequence of fields separated by '#'.
// The fields are:
//   0: valid (1 or 0)
//   1: msg_type (as int)
//   2: sender (serialized ReplicaID using '@')
//   3: currentTerm
//   4: lastTerm
//   5: prefixTerm
//   6: prefixLen
//   7: commitLength
//   8: logLength
//   9: granted (1 or 0)
//   10: suffix_count (number of log entries in suffix)
// Then for each LogEntry in suffix, add two fields:
//   (term, and msg (serialized))
// Then add:
//   next: ack
//   last: success (1 or 0)
string serializeRaftQuery(const RaftQuery &rq)
{
    stringstream ss;
    ss << (rq.valid ? "1" : "0") << "#"
       << to_string(rq.msg_type) << "#"
       << serializeReplicaID(rq.sender) << "#"
       << rq.currentTerm << "#"
       << rq.lastTerm << "#"
       << rq.prefixTerm << "#"
       << rq.prefixLen << "#"
       << rq.commitLength << "#"
       << rq.logLength << "#"
       << (rq.granted ? "1" : "0") << "#";
    ss << rq.suffix.size();
    for (size_t i = 0; i < rq.suffix.size(); i++)
    {
        ss << "#" << rq.suffix[i].term << "#" << serializeString(rq.suffix[i].msg);
    }
    ss << "#" << rq.ack << "#"
       << (rq.success ? "1" : "0");
    return ss.str();
}

RaftQuery deserializeRaftQuery(const string &data)
{
    RaftQuery rq;
    vector<string> fields = splitString(data, '#');
    // There must be at least 11 fields before the suffix list.
    if (fields.size() < 11)
    {
        return rq; // error: return default-constructed query
    }
    rq.valid = (fields[0] == "1");
    rq.msg_type = static_cast<Message>(stoi(fields[1]));
    rq.sender = deserializeReplicaID(fields[2]);
    rq.currentTerm = stoi(fields[3]);
    rq.lastTerm = stoi(fields[4]);
    rq.prefixTerm = stoi(fields[5]);
    rq.prefixLen = stoi(fields[6]);
    rq.commitLength = stoi(fields[7]);
    rq.logLength = stoi(fields[8]);
    rq.granted = (fields[9] == "1");
    size_t suffixCount = stoul(fields[10]);
    size_t expectedFields = 11 + suffixCount * 2 + 2;
    if (fields.size() < expectedFields)
    {
        return rq; // error: not enough fields
    }
    rq.suffix.clear();
    size_t index = 11;
    for (size_t i = 0; i < suffixCount; i++)
    {
        int term = stoi(fields[index++]);
        string msg = deserializeString(fields[index++]);
        rq.suffix.push_back(LogEntry(term, msg));
    }
    rq.ack = stoi(fields[index++]);
    rq.success = (fields[index++] == "1");
    return rq;
}

void printRaftQuery(const RaftQuery &rq)
{
    cout << "RaftQuery { valid: " << rq.valid
         << ", msg_type: " << rq.msg_type
         << ", sender: ";
    printReplicaID(rq.sender);
    cout << ", currentTerm: " << rq.currentTerm
         << ", lastTerm: " << rq.lastTerm
         << ", prefixTerm: " << rq.prefixTerm
         << ", prefixLen: " << rq.prefixLen
         << ", commitLength: " << rq.commitLength
         << ", logLength: " << rq.logLength
         << ", granted: " << rq.granted
         << ", suffix: [";
    for (size_t i = 0; i < rq.suffix.size(); i++)
    {
        printLogEntry(rq.suffix[i]);
        if (i != rq.suffix.size() - 1)
            cout << ", ";
    }
    cout << "], ack: " << rq.ack
         << ", success: " << rq.success
         << " }";
}

// ============================================================
// RequestQuery
// ------------------------------------------------------------
// Fields:
//   0: request_replica_id (serialized ReplicaID)
//   1: count of other_replica_id (number of entries in vector)
// Then for each other_replica_id, one field (serialized using ReplicaID)
// Next field: operation (int)
// Next: key (serialized string)
// Next: value (serialized string)
// Finally: raft_request (its entire serialized string, serialized so that inner '#' are preserved)
string serializeRequestQuery(const RequestQuery &rq)
{
    stringstream ss;
    ss << serializeReplicaID(rq.request_replica_id) << "#";
    ss << rq.other_replica_id.size();
    for (size_t i = 0; i < rq.other_replica_id.size(); i++)
    {
        ss << "#" << serializeReplicaID(rq.other_replica_id[i]);
    }
    ss << "#" << rq.operation << "#"
       << serializeString(rq.key) << "#"
       << serializeString(rq.value) << "#"
       << serializeString(serializeRaftQuery(rq.raft_request));
    return ss.str();
}

RequestQuery deserializeRequestQuery(const string &data)
{
    RequestQuery rq;
    vector<string> fields = splitString(data, '#');
    // Minimum fields: request_replica_id, count, operation, key, value, raft_request => 6 fields
    if (fields.size() < 6)
    {
        return rq;
    }
    rq.request_replica_id = deserializeReplicaID(fields[0]);
    size_t count = stoul(fields[1]);
    size_t index = 2;
    rq.other_replica_id.clear();
    for (size_t i = 0; i < count; i++)
    {
        if (index < fields.size())
        {
            rq.other_replica_id.push_back(deserializeReplicaID(fields[index++]));
        }
    }
    if (index >= fields.size())
        return rq;
    rq.operation = stoi(fields[index++]);
    if (index >= fields.size())
        return rq;
    rq.key = deserializeString(fields[index++]);
    if (index >= fields.size())
        return rq;
    rq.value = deserializeString(fields[index++]);
    if (index >= fields.size())
        return rq;
    string raftEscaped = fields[index++];
    string raftSerialized = deserializeString(raftEscaped);
    rq.raft_request = deserializeRaftQuery(raftSerialized);
    return rq;
}

void printRequestQuery(const RequestQuery &rq)
{
    cout << "RequestQuery { request_replica_id: ";
    printReplicaID(rq.request_replica_id);
    cout << ", other_replica_id: [";
    for (size_t i = 0; i < rq.other_replica_id.size(); i++)
    {
        printReplicaID(rq.other_replica_id[i]);
        if (i != rq.other_replica_id.size() - 1)
            cout << ", ";
    }
    cout << "], operation: " << operationToString(static_cast<Operation>(rq.operation))
         << ", key: " << rq.key
         << ", value: " << rq.value
         << ", raft_request: ";
    printRaftQuery(rq.raft_request);
    cout << " }\n\n";
    fflush(stdout);
}

// ============================================================
// ReplyResponse
// ------------------------------------------------------------
// Fields:
//   0: reponse_replica_id (serialized ReplicaID)
//   1: status (int)
//   2: value (serialized string)
string serializeReplyResponse(const ReplyResponse &rr)
{
    stringstream ss;
    ss << serializeReplicaID(rr.reponse_replica_id) << "#"
       << rr.status << "#"
       << serializeString(rr.value);
    return ss.str();
}

ReplyResponse deserializeReplyResponse(const string &data)
{
    ReplyResponse rr;
    vector<string> fields = splitString(data, '#');
    if (fields.size() < 3)
        return rr;
    rr.reponse_replica_id = deserializeReplicaID(fields[0]);
    rr.status = stoi(fields[1]);
    rr.value = deserializeString(fields[2]);
    return rr;
}

void printReplyResponse(const ReplyResponse &rr)
{
    cout << "ReplyResponse { reponse_replica_id: ";
    printReplicaID(rr.reponse_replica_id);
    cout << ", status: " << rr.status
         << ", value: " << rr.value
         << " }\n\n";
    fflush(stdout);
}

// -------------------------------------------------------------------------------
typedef struct RequestToReplica
{
    sem_t sem;
    Operation op;
    size_t key_len;
    char key[MAX_STR_SIZE];
    size_t val_len;
    char val[MAX_STR_SIZE];
    RaftQuery raft_query;

    // Member function to completely reset all members.
    void reset()
    {
        key_len = 0;
        val_len = 0;
        raft_query.reset();
    }
} RequestToReplica;

typedef struct ReplyFromReplica
{
    sem_t sem;
    ReturnStatus status;
    size_t key_len;
    char key[MAX_STR_SIZE];
    size_t val_len;
    char val[MAX_STR_SIZE];

    // Member function to completely reset all members.
    void reset()
    {
        key_len = 0;
        val_len = 0;
    }
} ReplyFromReplica;

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

typedef struct LogMessage
{
    Operation op;
    string key;
    string value;
    ReplicaID replicaID;
    string localTime;
} LogMessage;

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

// Function to return a nicely formatted string from a LogMessage struct.
string formatLogMessage(const LogMessage &msg)
{
    ostringstream oss;
    oss << "[" << msg.localTime << "] "
        << "Operation: " << operationToString(msg.op) << ", "
        << "Key: " << msg.key << ", "
        << "Value: " << msg.value << ", "
        << "Replica: (" << msg.replicaID.availability_zone_id << ", " << msg.replicaID.slot_id << ")";
    return oss.str();
}

// Serializes a LogMessage into a string.
// Format: <op>#<key>#<value>#<serialized_replicaID>#<localTime>
string serializeLogMessage(const LogMessage &msg)
{
    stringstream ss;
    ss << static_cast<int>(msg.op) << "#"
       << msg.key << "#"
       << msg.value << "#"
       << serializeReplicaID(msg.replicaID) << "#"
       << msg.localTime;
    return ss.str();
}

// Deserializes a string into a LogMessage object.
// Expects the same format as produced by serializeLogMessage.
LogMessage deserializeLogMessage(const string &s)
{
    LogMessage msg;
    stringstream ss(s);
    string token;

    // Extract operation.
    if (getline(ss, token, '#'))
    {
        msg.op = static_cast<Operation>(stoi(token));
    }
    // Extract key.
    if (getline(ss, token, '#'))
    {
        msg.key = token;
    }
    // Extract value.
    if (getline(ss, token, '#'))
    {
        msg.value = token;
    }
    // Extract ReplicaID.
    if (getline(ss, token, '#'))
    {
        msg.replicaID = deserializeReplicaID(token);
    }
    // Extract localTime (rest of the line).
    if (getline(ss, token))
    {
        msg.localTime = token;
    }

    return msg;
}

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

#endif // DB_H
