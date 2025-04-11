#include "../db.h"
#include "lsm.cpp" // External functions: start_compaction(), SET(), GET(), DEL()

#define MIN_ELECTION_TIMEOUT 4000 // milliseconds
#define MAX_ELECTION_TIMEOUT 8000 // milliseconds
#define WAKEUP_TIME 1000          // milliseconds
class Raft
{
public:
    Raft(ReplicaID self, int numPeers, vector<ReplicaID> &peers, ReplyFromReplica *reply_ptr_, int replica_sockfd_, map<ReplicaID, Address> &replicaInfo_)
        : nodeId(self),
          numPeers(numPeers),
          currentTerm(0),
          votedFor(nullReplica),
          commitLength(0),
          currentRole(FOLLOWER),
          currentLeader(nullReplica),
          votesReceived(),
          sentLength(numPeers, 0),
          ackedLength(numPeers, 0),
          nodes(peers),
          reply_ptr(reply_ptr_),
          replicaInfo(replicaInfo_),
          replicaSockfd(replica_sockfd_)
    {
        // initialise 2 threads.
        // 1. Periodic HEARTBEAT_MSSG and timeout expiry
        // 2. Listening for TCP connections over own socket
        // 3. Waiting on shared memory for user requests --> Not here, main() of replica_machine is doing this
        // initState();

        // mt = new MergeTree(to_string(nodeId.availability_zone_id) + "_" + to_string(nodeId.slot_id));
        // Address node_addr = replicaInfo[nodeId];
        // std::thread thread1(&Raft::runPeriodicTask, this);
        // std::thread thread2(&Raft::startTcpServer, this, node_addr);
        // thread1.join();
        // thread2.join();
    }

    ~Raft()
    {
        // Will implement later
        delete mt;
    }
    void print() const
    {
        cout << "nodeId         : (" << nodeId.slot_id << ", " << nodeId.availability_zone_id << ")" << endl;
        cout << "======== Raft State ========" << endl;
        cout << "numPeers       : " << numPeers << endl;
        cout << "currentTerm    : " << currentTerm << endl;
        cout << "votedFor       : ";
        votedFor.print();
        cout << endl;
        cout << "commitLength   : " << commitLength << endl;
        cout << "currentRole    : " << roleToString(currentRole) << endl;
        cout << "currentLeader  : ";
        currentLeader.print();
        cout << endl;

        cout << "votesReceived  : { ";
        for (ReplicaID vote : votesReceived)
        {
            vote.print();
            cout << " ";
        }
        cout << "}" << endl;

        cout << "sentLength     : [ ";
        for (int s : sentLength)
            cout << s << " ";
        cout << "]" << endl;

        cout << "ackedLength    : [ ";
        for (int a : ackedLength)
            cout << a << " ";
        cout << "]" << endl;

        cout << "Log Entries:" << endl;
        for (size_t i = 0; i < log.size(); i++)
        {
            cout << "  [" << i << "] Term: " << log[i].term
                 << ", Msg: " << log[i].msg << endl;
        }

        cout << "Nodes:" << endl;
        for (size_t i = 0; i < nodes.size(); i++)
        {
            cout << "  Node " << i << " -> slot_id: " << nodes[i].slot_id << ", availability_zone_id: " << nodes[i].availability_zone_id << endl;
        }
        cout << "============================" << endl;
    }

    void onReceiveUserRequest(RequestQuery &request)
    {
        cout << "hhhhhhhhhhhhhhhhhhhhhh" << endl;
        /////// Need to fix !!!!
        std::lock_guard<std::mutex> lock(mtx);
        cout << "Received User Request" << endl;
        request.print();
        cout << endl;
        // LogMessage logmsg;
        // logmsg.op = request.operation;
        // logmsg.key = string(request.key, request.key_len);
        // logmsg.value = string(request.value, request.value_len);
        // logmsg.replicaID = nodeId;
        // logmsg.localTime = getCurrentLocalTime();
        // if (currentRole == LEADER)
        // {
        //     // Append the record to log
        //     log.emplace_back(LogEntry(currentTerm, logmsg.format()));

        //     // Acknowledge for self
        //     ackedLength[nodeId.slot_id] = log.size();

        //     // Replicate to all followers
        //     for (const ReplicaID &follower : nodes)
        //     {
        //         if (follower.slot_id != nodeId.slot_id)
        //         {
        //             replicateLog(follower);
        //         }
        //     }
        // }
        // else
        // {
        //     RaftQuery raftQuery;
        //     raftQuery.reset();
        //     raftQuery.request_query = request;
        //     raftQuery.msg_type = Message::UserRequest;
        //     cout << "Sending request to Leader : ";
        //     currentLeader.print();
        //     cout << endl;
        //     sendReplica(replicaInfo[currentLeader], raftQuery.serialize());
        // }
    }

private:
    ReplicaID nodeId;
    int numPeers;
    int currentTerm;
    ReplicaID votedFor;
    vector<LogEntry> log;
    int commitLength;
    Role currentRole;
    ReplicaID currentLeader;
    set<ReplicaID> votesReceived;
    vector<int> sentLength;
    vector<int> ackedLength;
    vector<ReplicaID> nodes;
    MergeTree *mt;
    thread periodicThread;
    mutex mtx;
    chrono::steady_clock::time_point electionDeadline;
    chrono::steady_clock::time_point heartBeatDeadline;
    RequestToReplica *request_ptr;
    ReplyFromReplica *reply_ptr;
    map<ReplicaID, Address> replicaInfo;
    int replicaSockfd;

    void startTcpServer(const Address &node_addr)
    {

        // Start listening (with a backlog of 10).
        cout << "replicaSockfd : " << replicaSockfd << endl;
        if (listen(replicaSockfd, 10) < 0)
        {
            perror("listen failed");
            close(replicaSockfd);
            return;
        }

        std::cout << "TCP server listening on " << node_addr.host << ":" << node_addr.port << std::endl;

        while (true)
        {
            struct sockaddr_in clientAddr;
            socklen_t clientLen = sizeof(clientAddr);
            int client_fd = accept(replicaSockfd, (struct sockaddr *)&clientAddr, &clientLen);
            if (client_fd < 0)
            {
                perror("accept failed");
                continue;
            }
            // Buffer to store the client IP address in string format.
            char clientIP[INET_ADDRSTRLEN];
            // Convert the client's IP address to string form.
            if (inet_ntop(AF_INET, &clientAddr.sin_addr, clientIP, sizeof(clientIP)) == NULL)
            {
                perror("inet_ntop failed");
                // Optionally handle error
            }

            // Convert the port number from network byte order to host byte order.
            int clientPort = ntohs(clientAddr.sin_port);

            // Print the client's IP address and port.
            std::cout << "Accepted a connection." << std::endl;
            printf("Client connected from IP: %s, Port: %d\n", clientIP, clientPort);
            // Here you may handle the client connection as needed.
            // For demonstration, we immediately close the connection.
            string result;
            recv_all(client_fd, result);
            RaftQuery raftQuery = RaftQuery::deserialize(result);
            raftQuery.print();
            onReceiveRaftQuery(raftQuery);
            close(client_fd);
        }

        // We never reach here
        close(replicaSockfd);
    }

    void onReceiveRaftQuery(RaftQuery &request)
    {
        if (!request.valid)
        {

            std::cerr << "Received invalid RaftQuery from sender "
                      << request.sender.slot_id << std::endl;
            return;
        }
        else
        {
            switch (request.msg_type)
            {
            case VoteRequest:
                onReceiveVoteRequest(request.sender, request.currentTerm,
                                     request.logLength, request.lastTerm);
                break;
            case VoteResponse:
                onReceiveVoteResponse(request.sender, request.currentTerm,
                                      request.granted);
                break;
            case LogRequest:
                onReceiveLogRequest(request.sender, request.currentTerm,
                                    request.prefixLen, request.prefixTerm,
                                    request.commitLength, request.suffix);
                break;
            case LogResponse:
                onReceiveLogResponse(request.sender, request.currentTerm,
                                     request.ack, request.success);
                break;
            case UserRequest:
                onReceiveUserRequest(request.request_query);
            default:
                std::cerr << "Unknown RaftQuery message type." << std::endl;
                break;
            }
        }
    }

    bool sendRaftQuery(RaftQuery &request, const ReplicaID &destination)
    {
        cout << "Sending RaftQuery to destination: ";
        request.print();
        cout << "Destination: ";
        destination.print();
        string strRaftQuery = request.serialize();
        return sendReplica(replicaInfo[destination], strRaftQuery);
    }
    bool sendReplica(Address dest_addr, const string &msg)
    {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0)
        {
            perror("Socket creation failed");
            return 1;
        }

        // Set up the destination socket address structure.
        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(dest_addr.port);
        if (inet_pton(AF_INET, dest_addr.host.c_str(), &serverAddr.sin_addr) <= 0)
        {
            perror("In RaftModule, Invalid address/ Address not supported");
            close(sockfd);
            return 1;
        }

        // Connect to the server.
        if (connect(sockfd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0)
        {
            cout << "Error connecting to server: " << dest_addr.host << ":" << dest_addr.port << endl;
            perror("Connection Failed");
            close(sockfd);
            return 1;
        }
        bool done = send_all(sockfd, msg);
        close(sockfd);
        return done;
    }
    int randomElectionTimeout()
    {
        return MIN_ELECTION_TIMEOUT + (rand() % (MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT)); // 300 to 500 ms
    }
    // void writeToStableStorage()
    // {
    //     ofstream outfile(serializeReplicaID(nodeId) + "raft_file.txt");
    //     if (!outfile.is_open())
    //     {
    //         cerr << "Error opening raft_file.txt for writing!" << endl;
    //         return;
    //     }

    //     outfile << nodeId.slot_id << " " << nodeId.availability_zone_id << endl;
    //     // Write nodeId

    //     // Write currentTerm, votedFor, commitLength, numPeers
    //     outfile << currentTerm << " " << votedFor << " " << commitLength << " " << numPeers << endl;

    //     // Write log entries
    //     outfile << log.size() << endl;
    //     for (const auto &entry : log)
    //     {
    //         outfile << entry.term << " " << entry.msg << endl;
    //     }

    //     // Write nodes (followers)
    //     for (const auto &f : nodes)
    //         outfile << f.slot_id << " " << f.availability_zone_id << endl;
    //     {
    //     }

    //     outfile.close();
    // }

    void runPeriodicTask()
    {
        resetElectionTimer();
        while (true)
        {
            // writeToStableStorage();
            this_thread::sleep_for(chrono::milliseconds(WAKEUP_TIME)); // wakes up periodically
            print();
            {
                // lock_guard<mutex> lock(mtx);
                cout << "Periodic task running..." << endl;
                if (currentRole == LEADER)
                {
                    cout << "Periodic task running...1 ...... REPLICATING LOG" << endl;
                    for (const ReplicaID &follower : nodes)
                    {
                        if (follower.slot_id != nodeId.slot_id)
                        {
                            replicateLog(follower);
                        }
                    }
                }
                else
                {
                    cout << "Periodic task running...3 .... Not a leader" << endl;
                    if (chrono::steady_clock::now() >= electionDeadline)
                    {
                        cout << "Deadline expired ..... Starting a Leader Election" << endl;
                        resetElectionTimer();
                        currentTerm += 1;
                        currentRole = CANDIDATE;
                        votedFor = nodeId;
                        votesReceived.clear();
                        votesReceived.insert(nodeId);

                        int lastTerm = 0;
                        if (!log.empty())
                            lastTerm = log.back().term;

                        // cout << "Election timeout: starting election for term " << currentTerm << endl;
                        // cout << "Sending VoteRequest: (VoteRequest, " << nodeId.slot_id << ", "
                        //      << currentTerm << ", " << log.size() << ", " << lastTerm << ")" << endl;
                        for (const ReplicaID &other_node : nodes)
                        {
                            // cout << "   Sending VoteRequest to node " << ReplicaID.slot_id << endl;
                            RaftQuery request;
                            request.valid = true;
                            request.msg_type = VoteRequest; // Assuming VoteResponse is a valid Message enum value
                            request.sender = nodeId;        // This node is the sender of the response
                            request.currentTerm = currentTerm;
                            request.lastTerm = lastTerm;
                            request.prefixTerm = 0;              // Not applicable for vote responses
                            request.prefixLen = 0;               // Not applicable for vote responses
                            request.commitLength = commitLength; // Assuming commitLength is a defined member variable
                            request.logLength = log.size();
                            request.granted = 0;
                            request.suffix.clear(); // No log entries needed in the vote response
                            request.ack = 0;        // Not used in vote responses
                            request.success = 0;    // Reflecting the vote decision
                            request.request_query.reset();
                            sendRaftQuery(request, other_node);
                        }
                    }
                }
            }
        }
    }

    void resetElectionTimer()
    {
        lock_guard<mutex> lock(mtx);
        electionDeadline = chrono::steady_clock::now() + chrono::milliseconds(randomElectionTimeout());
    }

    string roleToString(Role r) const
    {
        switch (r)
        {
        case FOLLOWER:
            return "FOLLOWER";
        case CANDIDATE:
            return "CANDIDATE";
        case LEADER:
            return "LEADER";
        default:
            return "UNKNOWN";
        }
    }

    // void initState()
    // {
    //     ifstream infile(serializeReplicaID(nodeId) + "raft_file.txt");
    //     if (infile.good())
    //         infile >> nodeId.slot_id >> nodeId.availability_zone_id;
    //     {
    //         infile >> currentTerm;
    //         infile >> votedFor;
    //         infile >> commitLength;
    //         infile >> numPeers;

    //         int logSize;
    //         infile >> logSize;
    //         infile.ignore();
    //         log.clear();
    //         for (int i = 0; i < logSize; i++)
    //         {
    //             int term;
    //             string msg;
    //             string line;
    //             getline(infile, line);
    //             istringstream iss(line);
    //             iss >> term;
    //             getline(iss, msg);
    //             if (!msg.empty() && msg[0] == ' ')
    //                 msg.erase(0, 1);
    //             log.push_back(LogEntry(term, msg));
    //         }

    //         nodes.clear();
    //         int nodeCount = numPeers - 1;
    //         for (int i = 0; i < nodeCount; i++)
    //         {
    //             int slot_id, availability_zone_id;
    //             infile >> slot_id >> availability_zone_id;
    //             nodes.push_back(ReplicaID{slot_id, availability_zone_id});
    //         }

    //         sentLength = vector<int>(numPeers, 0);
    //         ackedLength = vector<int>(numPeers, 0);
    //         votesReceived.clear();
    //         currentRole = FOLLOWER;
    //         currentLeader = nullReplica;

    //         infile.close();
    //     }
    // }

    void onReceiveVoteRequest(ReplicaID &cId, int cTerm, int cLogLength, int cLogTerm)
    {
        if (cTerm > currentTerm)
        {
            currentTerm = cTerm;
            currentRole = FOLLOWER;
            votedFor = nullReplica;
        }
        int lastTerm = 0;
        if (!log.empty())
            lastTerm = log.back().term;

        bool logOk = (cLogTerm > lastTerm) || (cLogTerm == lastTerm && cLogLength >= static_cast<int>(log.size()));
        bool voteGranted = false;
        if (cTerm == currentTerm && logOk && (votedFor == cId || votedFor == nullReplica))
        {
            votedFor = cId;
            voteGranted = true;
            // cout << "Sending VoteResponse: (VoteResponse, " << nodeId.slot_id << ", " << currentTerm << ", true) to node " << cId << endl;
        }
        else
        {
            // cout << "Sending VoteResponse: (VoteResponse, " << nodeId.slot_id << ", " << currentTerm << ", false) to node " << cId << endl;
        }
        RaftQuery request;
        request.valid = true;
        request.msg_type = Message::VoteResponse; // Assuming VoteResponse is a valid Message enum value
        request.sender = nodeId;                  // This node is the sender of the response
        request.currentTerm = currentTerm;
        request.lastTerm = lastTerm;
        request.prefixTerm = 0;              // Not applicable for vote responses
        request.prefixLen = 0;               // Not applicable for vote responses
        request.commitLength = commitLength; // Assuming commitLength is a defined member variable
        request.logLength = log.size();
        request.granted = voteGranted;
        request.suffix.clear();        // No log entries needed in the vote response
        request.ack = 0;               // Not used in vote responses
        request.success = voteGranted; // Reflecting the vote decision
        request.request_query.reset();
        sendRaftQuery(request, cId);
    }

    void onReceiveVoteResponse(ReplicaID &voterId, int term, bool granted)
    {
        cout << "Processing Vote Response" << endl;
        if (currentRole == CANDIDATE && term == currentTerm && granted)
        {
            cout << "Yeah, I got a vote from ";
            voterId.print();
            cout << endl;
            votesReceived.insert(voterId);
            if ((votesReceived.size() + 1) >= (numPeers / 2 + 1))
            {
                currentRole = LEADER;
                currentLeader = nodeId;
                resetElectionTimer();
                for (size_t i = 0; i < nodes.size(); i++)
                {
                    sentLength[i] = static_cast<int>(log.size());
                    ackedLength[i] = 0;
                    replicateLog(nodes[i]);
                }
            }
        }
        else if (term > currentTerm)
        {
            cout << "I am outdated" << endl;
            currentTerm = term;
            currentRole = FOLLOWER;
            votedFor = nullReplica;
            resetElectionTimer();
        }
    }

    void replicateLog(const ReplicaID &followerId)
    {
        int prefixLen = sentLength[followerId.slot_id];
        vector<LogEntry> suffix;

        for (size_t i = prefixLen; i < log.size(); ++i)
        {
            suffix.push_back(log[i]);
        }

        int prefixTerm = 0;
        if (prefixLen > 0)
        {
            prefixTerm = log[prefixLen - 1].term;
        }

        // // Simulate sending (LogRequest, leaderId, currentTerm, prefixLen,
        // // prefixTerm, commitLength, suffix) to followerId
        // cout << "Sending LogRequest to follower " << followerId.slot_id << ":\n";
        // cout << "  Leader ID     : " << nodeId << endl;
        // cout << "  Term          : " << currentTerm << endl;
        // cout << "  Prefix Length : " << prefixLen << endl;
        // cout << "  Prefix Term   : " << prefixTerm << endl;
        // cout << "  Commit Length : " << commitLength << endl;
        // cout << "  Suffix        : [";
        // for (const auto &entry : suffix)
        // {
        //     cout << " (Term: " << entry.term << ", Msg: " << entry.msg << ")";
        // }
        // cout << " ]\n";

        RaftQuery request;
        request.valid = true;
        request.msg_type = Message::LogRequest; // Assuming LogRequest is a valid Message enum value
        request.sender = nodeId;                // Constructing a ReplicaID from leaderId
        request.currentTerm = currentTerm;
        request.lastTerm = (log.empty() ? 0 : log.back().term);
        request.prefixTerm = prefixTerm;
        request.prefixLen = prefixLen;
        request.commitLength = commitLength;
        request.logLength = log.size();
        request.granted = false; // Not applicable for log replication
        request.suffix = suffix;
        request.ack = 0;         // Not used for log replication
        request.success = false; // Not applicable for log replication
        request.request_query.reset();
        sendRaftQuery(request, followerId);
    }

    void onReceiveLogRequest(ReplicaID &leaderId, int term, int prefixLen, int prefixTerm,
                             int leaderCommit, const vector<LogEntry> &suffix)
    {
        if (term > currentTerm)
        {
            currentTerm = term;
            votedFor = nullReplica;
            resetElectionTimer();
        }

        if (term == currentTerm)
        {
            currentRole = FOLLOWER;
            currentLeader = leaderId;
        }

        bool logOk = (log.size() >= static_cast<size_t>(prefixLen)) &&
                     (prefixLen == 0 || log[prefixLen - 1].term == prefixTerm);

        RaftQuery request;
        request.valid = true;
        request.sender = nodeId; // This node sends the response
        request.currentTerm = currentTerm;
        request.lastTerm = (log.empty() ? 0 : log.back().term);
        request.prefixTerm = prefixTerm;
        request.prefixLen = prefixLen;
        request.commitLength = leaderCommit; // Reflecting leader's commit length
        request.logLength = log.size();
        request.suffix.clear(); // Not sending any log entries in the response

        if (term == currentTerm && logOk)
        {
            appendEntries(prefixLen, leaderCommit, suffix);
            int ack = prefixLen + suffix.size();

            // cout << "Sending LogResponse to leader " << leaderId << ":\n";
            // cout << "  From Node     : " << nodeId.slot_id << endl;
            // cout << "  Term          : " << currentTerm << endl;
            // cout << "  Ack           : " << ack << endl;
            // cout << "  Success       : true\n";

            request.ack = ack;
            request.success = true;
            request.msg_type = LogResponse; // Assuming LogResponse is a valid Message enum value
        }
        else
        {
            // cout << "Sending LogResponse to leader " << leaderId << ":\n";
            // cout << "  From Node     : " << nodeId.slot_id << endl;
            // cout << "  Term          : " << currentTerm << endl;
            // cout << "  Ack           : 0\n";
            // cout << "  Success       : false\n";

            request.ack = 0;
            request.success = false;
            request.msg_type = LogResponse; // Assuming LogResponse is a valid Message enum value
        }
        request.request_query.reset();
        sendRaftQuery(request, leaderId);
    }

    void appendEntries(int prefixLen, int leaderCommit, const vector<LogEntry> &suffix)
    {
        // Conflict check: truncate conflicting suffix
        if (!suffix.empty() && log.size() > static_cast<size_t>(prefixLen))
        {
            int index = min(static_cast<int>(log.size()), prefixLen + static_cast<int>(suffix.size())) - 1;
            if (log[index].term != suffix[index - prefixLen].term)
            {
                log.resize(prefixLen); // Truncate log from prefixLen onward
            }
        }

        // Append new suffix entries if needed
        if (prefixLen + static_cast<int>(suffix.size()) > static_cast<int>(log.size()))
        {
            for (int i = static_cast<int>(log.size()) - prefixLen; i < static_cast<int>(suffix.size()); ++i)
            {
                log.push_back(suffix[i]);
            }
        }

        // Apply committed entries
        if (leaderCommit > commitLength)
        {
            for (int i = commitLength; i < min(leaderCommit, static_cast<int>(log.size())); ++i)
            {
                // Simulate delivering to application
                // cout << "Delivering to application: " << log[i].msg << endl;
            }
            commitLength = min(leaderCommit, static_cast<int>(log.size()));
        }
    }

    void onReceiveLogResponse(ReplicaID &followerId, int term, int ack, bool success)
    {
        if (term == currentTerm && currentRole == LEADER)
        {
            if (success && ack >= ackedLength[followerId.slot_id])
            {
                sentLength[followerId.slot_id] = ack;
                ackedLength[followerId.slot_id] = ack;
                commitLogEntries(); // This function must be implemented separately
            }
            else if (sentLength[followerId.slot_id] > 0)
            {
                sentLength[followerId.slot_id]--;
                replicateLog(followerId); // Re-send with decremented prefix
            }
        }
        else if (term > currentTerm)
        {
            currentTerm = term;
            currentRole = FOLLOWER;
            votedFor = nullReplica;
            resetElectionTimer();
        }
    }
    int replyToJobManager(LogMessage &query, ReturnStatus status)
    {
        if (query.replicaID == nodeId)
        {
            reply_ptr->reply.reponse_replica_id = nodeId;
            reply_ptr->reply.status = status;
            // Copy key from query to reply_ptr.
            // reply_ptr->reply.key_len = query.key.size();
            // memcpy(reply_ptr->reply.key, query.key.c_str(), reply_ptr->reply.key_len);
            // Ensure null-termination for safety.
            // reply_ptr->key[reply_ptr->key_len] = '\0';

            // Copy value from query to reply_ptr.
            reply_ptr->reply.value_len = query.value.size();
            memcpy(reply_ptr->reply.value, query.value.c_str(), reply_ptr->reply.value_len);
            reply_ptr->reply.value[reply_ptr->reply.value_len] = '\0';

            if (sem_post(&(reply_ptr->sem)) == -1)
            {
                perror("At RaftModule, sem_post");
                return EXIT_FAILURE;
            }
        }
        return EXIT_SUCCESS;
    }
    int COMMIT(const string &msg)
    {
        cout << "COMMittting ***************************************" << endl;
        LogMessage query = LogMessage::deserialize(msg);
        query.print();
        if (query.op == Operation::DEL)
        {
            ReturnStatus status = mt->DEL(query.key);
            replyToJobManager(query, status);
        }
        else if (query.op == Operation::SET)
        {
            ReturnStatus status = mt->SET(query.key, query.value);
            replyToJobManager(query, status);
        }
        else if (query.op == Operation::GET)
        {
            pair<ReturnStatus, string> result = mt->GET(query.key);
            query.value = result.second;
            replyToJobManager(query, result.first);
        }
        else
        {
            cerr << "Invalid Operation at ReplicaMachine" << endl;
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    void commitLogEntries()
    {
        int minAcks = (static_cast<int>(nodes.size()) + 1 + 1) / 2; // ceil((n+1)/2)

        vector<int> ready;

        for (int len = 1; len <= static_cast<int>(log.size()); ++len)
        {
            int count = 1; // include self
            for (size_t i = 0; i < nodes.size(); ++i)
            {
                if (ackedLength[i] >= len)
                    count++;
            }

            if (count >= minAcks)
                ready.push_back(len);
        }

        if (!ready.empty())
        {
            int maxReady = *max_element(ready.begin(), ready.end());

            if (maxReady > commitLength && log[maxReady - 1].term == currentTerm)
            {
                for (int i = commitLength; i < maxReady; ++i)
                {
                    COMMIT(log[i].msg);
                }
                commitLength = maxReady;
            }
        }
    }
};