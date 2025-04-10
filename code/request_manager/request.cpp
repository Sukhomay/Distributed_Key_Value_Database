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

// #include "CONFIG.h"
// #include "request.h"
#include "../db.h"

// ReturnStatus createTable(TableAttr attr)
// {
//     try
//     {
//         vector<Partition> partitions = makeOptimalPartitions(attr); // atomically make partitions and save info to metadata
//         return ReturnStatus::SUCCESS;
//     }
//     catch(const std::exception& e)
//     {
//         std::cerr << e.what() << '\n';
//         return ReturnStatus::FAILURE;
//     }
// }

// ReturnStatus deleteTable()
// {

// }

// ReturnStatus put(string &key, string &value)
// {
//     try
//     {
//         Partition partition_id = consistentHash(key);
//         partition_id.put(key,value);
//         return ReturnStatus::SUCCESS;
//     }
//     catch(const std::exception& e)
//     {
//         std::cerr << e.what() << '\n';
//         return ReturnStatus::FAILURE;
//     }
// }

// pair<ReturnStatus, string> get(string &key)
// {
//     try
//     {
//         Partition partition_id = consistentHash(key);
//         string value = partition_id.get(key);
//         return make_pair(ReturnStatus::SUCCESS, value);
//     }
//     catch(const std::exception& e)
//     {
//         std::cerr << e.what() << '\n';
//         return make_pair(ReturnStatus::FAILURE, "");
//     }
// }

// vector<pair<string, string>> list(string &lower_bound_key, string &upper_bound_key)
// {

// }

#include <bits/stdc++.h>


int main()
{

    // 1. Create a socket.
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        perror("socket creation failed");
        return 0;
    }

    string server_ip = "127.0.0.1";
    int server_port = 7000;
    // 2. Setup server address.
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0)
    {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        close(sockfd);
        return 0;
    }

    // 3. Connect to the server.
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("Connection Failed");
        close(sockfd);
        return 0;
    }
    std::cout << "Connected to " << server_ip << ":" << server_port << std::endl;

    string resp;
    recv_all(sockfd, resp);

    cout << "resp : " << resp << endl;

    send_all(sockfd, "OK");

    close(sockfd);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        perror("socket creation failed");
        return 0;
    }

    int new_port = extractPort(resp);
    server_port = new_port;
    serv_addr.sin_port = htons(server_port);
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0)
    {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        close(sockfd);
        return 0;
    }

    while (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        /* code */
    }

    recv_all(sockfd, resp);

    cout << resp << endl;

    RequestQuery req;
    req.operation = Operation::CREATE_PROPAGATE;
    req.request_replica_id = ReplicaID{0, 3};
    req.sibling_replica_id.push_back(ReplicaID{1, 4});

    req.print();
    send_all(sockfd, req.serialize());


    recv_all(sockfd, resp);

    ReplyResponse reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::SET;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = req.value_len = 3;
    memcpy(req.key, "aaa", 3);
    memcpy(req.value, "AAA", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::SET;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = req.value_len = 3;
    memcpy(req.key, "ccc", 3);
    memcpy(req.value, "CCC", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::GET;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = 3;
    memcpy(req.key, "aaa", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::GET;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = 3;
    memcpy(req.key, "bbb", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::DEL;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = 3;
    memcpy(req.key, "ccc", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();

    req.operation = Operation::GET;
    req.request_replica_id = ReplicaID{0, 2};
    req.key_len = 3;
    memcpy(req.key, "ccc", 3);

    send_all(sockfd, req.serialize());

    recv_all(sockfd, resp);

    reply_resp = ReplyResponse::deserialize(resp);
    reply_resp.print();
}
