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
#define MAXLINE 1024
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
    GET,
    SET,
    DEL,
    CREATE,
    RAFT
};
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
    bool operator==(const ReplicaID &replica)
    {
        return (availability_zone_id == replica.availability_zone_id && slot_id == replica.slot_id);
    }
    bool operator<(const ReplicaID &other) const
    {
        return (availability_zone_id < other.availability_zone_id) || (availability_zone_id == other.availability_zone_id && slot_id < other.slot_id);
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
} RaftQuery;

typedef struct RequestQuery
{
    ReplicaID request_replica_id;
    vector<ReplicaID> other_replica_id;
    int operation;
    string key;
    string value;
    RaftQuery raft_request;
} RequestQuery;

typedef struct ReplyResponse
{
    ReplicaID reponse_replica_id;
    int status;
    string value;
} ReplyResponse;

// ============================================================
// Escape/unescape functions for strings using only '#' and '@'
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
                output.push_back('@'); // unknown escape: output '@'
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
// Utility: simple split on a character delimiter (no escape handling)
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
// LogEntry
// ------------------------------------------------------------
// Fields: term (int) and msg (string). We use '#' as the field delimiter,
// but we escape the string field.
string serializeLogEntry(const LogEntry &entry)
{
    return to_string(entry.term) + "#" + escapeString(entry.msg);
}

LogEntry deserializeLogEntry(const string &data)
{
    vector<string> parts = splitString(data, '#');
    int term = 0;
    string msg;
    if (parts.size() >= 2)
    {
        term = stoi(parts[0]);
        msg = unescapeString(parts[1]);
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
//   (term, and msg (escaped))
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
        ss << "#" << rq.suffix[i].term << "#" << escapeString(rq.suffix[i].msg);
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
        string msg = unescapeString(fields[index++]);
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
// Next: key (escaped string)
// Next: value (escaped string)
// Finally: raft_request (its entire serialized string, escaped so that inner '#' are preserved)
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
       << escapeString(rq.key) << "#"
       << escapeString(rq.value) << "#"
       << escapeString(serializeRaftQuery(rq.raft_request));
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
    rq.key = unescapeString(fields[index++]);
    if (index >= fields.size())
        return rq;
    rq.value = unescapeString(fields[index++]);
    if (index >= fields.size())
        return rq;
    string raftEscaped = fields[index++];
    string raftSerialized = unescapeString(raftEscaped);
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
    cout << "], operation: " << rq.operation
         << ", key: " << rq.key
         << ", value: " << rq.value
         << ", raft_request: ";
    printRaftQuery(rq.raft_request);
    cout << " }";
}

// ============================================================
// ReplyResponse
// ------------------------------------------------------------
// Fields:
//   0: reponse_replica_id (serialized ReplicaID)
//   1: status (int)
//   2: value (escaped string)
string serializeReplyResponse(const ReplyResponse &rr)
{
    stringstream ss;
    ss << serializeReplicaID(rr.reponse_replica_id) << "#"
       << rr.status << "#"
       << escapeString(rr.value);
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
    rr.value = unescapeString(fields[2]);
    return rr;
}

void printReplyResponse(const ReplyResponse &rr)
{
    cout << "ReplyResponse { reponse_replica_id: ";
    printReplicaID(rr.reponse_replica_id);
    cout << ", status: " << rr.status
         << ", value: " << rr.value
         << " }";
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
} RequestToReplica;

typedef struct ReplyFromReplica
{
    sem_t sem;
    ReturnStatus status;
    size_t val_len;
    char val[MAX_STR_SIZE];
} ReplyFromReplica;

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
