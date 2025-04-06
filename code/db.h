#ifndef DB_H
#define DB_H

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <vector>
#include <string>
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

using namespace std;


#define MAX_STR_SIZE 1024 // Assume this as the maximum size of key or value
#define JOB_REP_SHM_NAME "/jobmanager_replica_comm"
#define JOB_MANAGER_PORT 7000
#define MAXLINE 1024
#define MAX_EVENTS 10

enum class ReturnStatus
{
    FAILURE = 0,
    SUCCESS = 1
};

enum Operation
{
    CREATE,
    GET,
    SET,
    DEL
};

// -------------------------------------------------------------------------------
typedef struct ReplicaID
{
    int availability_zone_id;
    int slot_id;
} ReplicaID;

typedef struct RequestQuery
{
    ReplicaID request_replica_id;
    vector<ReplicaID> other_replica_id;
    int operation;
    string key;
    string value;
} RequestQuery;

typedef struct ReplyResponse
{
    ReplicaID reponse_replica_id;
    int status;
    string value;
} ReplyResponse;

// ------------------ ReplicaID Serialization ------------------

// Serialize a ReplicaID as "availability_zone_id,slot_id"
string serializeReplicaID(const ReplicaID &id)
{
    ostringstream oss;
    oss << id.availability_zone_id << "," << id.slot_id;
    return oss.str();
}

// Deserialize a ReplicaID from a string in the format "availability_zone_id,slot_id"
ReplicaID deserializeReplicaID(const string &s)
{
    ReplicaID id;
    size_t pos = s.find(',');
    if (pos == string::npos)
    {
        throw runtime_error("Invalid ReplicaID serialization: missing comma");
    }
    try
    {
        id.availability_zone_id = stoi(s.substr(0, pos));
        id.slot_id = stoi(s.substr(pos + 1));
    }
    catch (...)
    {
        throw runtime_error("Invalid ReplicaID serialization: conversion error");
    }
    return id;
}

// ------------------ Vector<ReplicaID> Serialization ------------------

// Serialize a vector<ReplicaID> as "<count>|<serialized_replica1>|<serialized_replica2>|..."
string serializeReplicaIDVector(const vector<ReplicaID> &vec)
{
    ostringstream oss;
    oss << vec.size();
    for (const auto &id : vec)
    {
        oss << "|" << serializeReplicaID(id);
    }
    return oss.str();
}

// Deserialize a vector<ReplicaID> from the format produced above.
vector<ReplicaID> deserializeReplicaIDVector(const string &s)
{
    vector<ReplicaID> vec;
    size_t pos = 0;
    size_t next = s.find('|', pos);
    if (next == string::npos)
    {
        // No pipe found: it should be an empty vector.
        int count = stoi(s);
        if (count != 0)
        {
            throw runtime_error("Invalid serialized vector: count non-zero but no items found");
        }
        return vec;
    }
    int count = stoi(s.substr(pos, next - pos));
    pos = next + 1;
    for (int i = 0; i < count; i++)
    {
        next = s.find('|', pos);
        string token;
        if (next == string::npos)
        {
            token = s.substr(pos);
        }
        else
        {
            token = s.substr(pos, next - pos);
        }
        vec.push_back(deserializeReplicaID(token));
        if (next == string::npos)
            break;
        pos = next + 1;
    }
    if (vec.size() != static_cast<size_t>(count))
    {
        throw runtime_error("Mismatch in replica vector count");
    }
    return vec;
}

// ------------------ String Serialization ------------------

// Serialize a string with length prefix: "<length>:<string>"
// This ensures that any delimiter in the string does not break deserialization.
string serializeString(const string &str)
{
    ostringstream oss;
    oss << str.size() << ":" << str;
    return oss.str();
}

// Deserialize a length-prefixed string starting at position 'pos' in s.
// Updates 'pos' to point after the deserialized string.
string deserializeString(const string &s, size_t &pos)
{
    size_t colonPos = s.find(':', pos);
    if (colonPos == string::npos)
    {
        throw runtime_error("Invalid serialized string: missing colon");
    }
    int len = stoi(s.substr(pos, colonPos - pos));
    pos = colonPos + 1;
    if (pos + len > s.size())
    {
        throw runtime_error("Invalid serialized string: length exceeds input size");
    }
    string result = s.substr(pos, len);
    pos += len;
    return result;
}

// ------------------ RequestQuery Serialization ------------------

// Serialize a RequestQuery using '#' as field delimiter.
// Fields are:
// 1. Serialized request_replica_id
// 2. Serialized other_replica_id (vector)
// 3. operation (as integer)
// 4. Serialized key (using length-prefix)
// 5. Serialized value (using length-prefix)
string serializeRequestQuery(const RequestQuery &rq)
{
    ostringstream oss;
    oss << serializeReplicaID(rq.request_replica_id);
    oss << "#" << serializeReplicaIDVector(rq.other_replica_id);
    oss << "#" << rq.operation;
    oss << "#" << serializeString(rq.key);
    oss << "#" << serializeString(rq.value);
    return oss.str();
}

// Deserialize a RequestQuery from the format above.
RequestQuery deserializeRequestQuery(const string &s)
{
    RequestQuery rq;
    size_t pos = 0;
    size_t next = s.find('#', pos);
    if (next == string::npos)
        throw runtime_error("Invalid RequestQuery serialization: missing fields (request_replica_id)");
    string replicaIDStr = s.substr(pos, next - pos);
    rq.request_replica_id = deserializeReplicaID(replicaIDStr);

    pos = next + 1;
    next = s.find('#', pos);
    if (next == string::npos)
        throw runtime_error("Invalid RequestQuery serialization: missing other_replica_id field");
    string vecStr = s.substr(pos, next - pos);
    rq.other_replica_id = deserializeReplicaIDVector(vecStr);

    pos = next + 1;
    next = s.find('#', pos);
    if (next == string::npos)
        throw runtime_error("Invalid RequestQuery serialization: missing operation field");
    try
    {
        rq.operation = stoi(s.substr(pos, next - pos));
    }
    catch (...)
    {
        throw runtime_error("Invalid RequestQuery serialization: operation conversion failed");
    }

    pos = next + 1;
    next = s.find('#', pos);
    if (next == string::npos)
        throw runtime_error("Invalid RequestQuery serialization: missing key field");
    // The key field is a serialized string (length-prefixed)
    string keySerialized = s.substr(pos, next - pos);
    size_t dummy = 0;
    rq.key = deserializeString(keySerialized, dummy);

    pos = next + 1;
    // The remaining part is the serialized value.
    string valueSerialized = s.substr(pos);
    dummy = 0;
    rq.value = deserializeString(valueSerialized, dummy);

    return rq;
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


#endif // DB_H
