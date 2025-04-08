#include "../db.h"

pair<string, int> getIP(const ReplicaID &replica)
{
    // For hardcoded loopback address with port based on slot_id.
    // For example, port = 5000 + replica.slot_id.
    return make_pair(string("127.0.0.1"), 5000 + replica.slot_id);
}
class Raft
{
public:
    Raft(ReplicaID self, int numPeers, vector<ReplicaID> &peers)
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
          nodes(peers)
    {
        initState();

        // For each peer, establish a TCP connection.
        for (size_t i = 0; i < peers.size(); i++)
        {
            // Skip connection to self.
            if (peers[i] == nodeId)
                continue;

            // Assume getIP returns a pair<string, int> representing IP address and port.
            auto [ip, port] = getIP(peers[i]);

            // Create a socket.
            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0)
            {
                cerr << "Socket creation failed for peer " << serializeReplicaID(peers[i]) << endl;
                continue;
            }

            // Set up the server address struct.
            sockaddr_in serv_addr;
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(port);
            if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0)
            {
                cerr << "Invalid address for peer " << serializeReplicaID(peers[i]) << endl;
                close(sockfd);
                continue;
            }

            // Connect to the peer.
            if (connect(sockfd, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            {
                cerr << "Connection failed for peer " << serializeReplicaID(peers[i]) << endl;
                close(sockfd);
                continue;
            }

            // Store the socket file descriptor.
            sockfds[peers[i]] = sockfd;
        }
    }

    void print() const
    {
        cout << "nodeId         : (" << nodeId.slot_id << ", " << nodeId.availability_zone_id << ")" << endl;
        cout << "======== Raft State ========" << endl;
        cout << "numPeers       : " << numPeers << endl;
        cout << "currentTerm    : " << currentTerm << endl;
        cout << "votedFor       : " << votedFor << endl;
        cout << "commitLength   : " << commitLength << endl;
        cout << "currentRole    : " << roleToString(currentRole) << endl;
        cout << "currentLeader  : " << currentLeader << endl;

        cout << "votesReceived  : { ";
        for (ReplicaID vote : votesReceived)
            cout << vote << " ";
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
            default:
                std::cerr << "Unknown RaftQuery message type." << std::endl;
                break;
            }
        }
    }

    void broadcastMessage(RequestQuery &request)
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (currentRole == LEADER)
        {
            // Append the record to log
            log.emplace_back(currentTerm, serializeRequestQuery(request));

            // Acknowledge for self
            ackedLength[nodeId.slot_id] = log.size();

            // Replicate to all followers
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
            // Forward to leader via FIFO (placeholder for actual networking)
            std::cout << "[Forwarding to Leader] "
                      << "LeaderID: " << currentLeader;
            printRequestQuery(request);

            // TODO: Implement actual message forwarding over network
            send_all(sockfds[currentLeader], serializeRequestQuery(request));
        }
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
    thread periodicThread;
    mutex mtx;
    chrono::steady_clock::time_point electionDeadline;
    map<ReplicaID, int> sockfds;

    bool sendRaftQuery(RaftQuery &request, const ReplicaID &destination)
    {
        // Actually need to send Request Query
        // Properly populate the string
        RequestQuery requestQuery;
        requestQuery.request_replica_id = nodeId;
        requestQuery.other_replica_id.clear();
        requestQuery.operation = RAFT;
        requestQuery.key = requestQuery.value = "";
        requestQuery.raft_request = request;
        string strRequestQuery = serializeRequestQuery(requestQuery);
        return send_all(sockfds[destination], strRequestQuery);
    }

    int randomElectionTimeout()
    {
        return 300 + (rand() % 201); // 300 to 500 ms
    }

    void writeToStableStorage()
    {
        ofstream outfile(serializeReplicaID(nodeId) + "raft_file.txt");
        if (!outfile.is_open())
        {
            cerr << "Error opening raft_file.txt for writing!" << endl;
            return;
        }

        outfile << nodeId.slot_id << " " << nodeId.availability_zone_id << endl;
        // Write nodeId

        // Write currentTerm, votedFor, commitLength, numPeers
        outfile << currentTerm << " " << votedFor << " " << commitLength << " " << numPeers << endl;

        // Write log entries
        outfile << log.size() << endl;
        for (const auto &entry : log)
        {
            outfile << entry.term << " " << entry.msg << endl;
        }

        // Write nodes (followers)
        for (const auto &f : nodes)
            outfile << f.slot_id << " " << f.availability_zone_id << endl;
        {
        }

        outfile.close();
    }

    void runPeriodicTask()
    {
        resetElectionTimer();
        while (true)
        {
            writeToStableStorage();
            this_thread::sleep_for(chrono::milliseconds(100)); // wakes up periodically

            lock_guard<mutex> lock(mtx);
            if (currentRole == LEADER)
            {
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
                if (chrono::steady_clock::now() >= electionDeadline)
                {
                    currentTerm += 1;
                    currentRole = CANDIDATE;
                    votedFor = nodeId;
                    votesReceived.clear();
                    votesReceived.insert(nodeId);

                    int lastTerm = 0;
                    if (!log.empty())
                        lastTerm = log.back().term;

                    cout << "Election timeout: starting election for term " << currentTerm << endl;
                    cout << "Sending VoteRequest: (VoteRequest, " << nodeId.slot_id << ", "
                         << currentTerm << ", " << log.size() << ", " << lastTerm << ")" << endl;
                    for (const ReplicaID &ReplicaID : nodes)
                    {
                        cout << "   Sending VoteRequest to node " << ReplicaID.slot_id << endl;
                    }
                    resetElectionTimer();
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

    void initState()
    {
        ifstream infile(serializeReplicaID(nodeId) + "raft_file.txt");
        if (infile.good())
            infile >> nodeId.slot_id >> nodeId.availability_zone_id;
        {
            infile >> currentTerm;
            infile >> votedFor;
            infile >> commitLength;
            infile >> numPeers;

            int logSize;
            infile >> logSize;
            infile.ignore();
            log.clear();
            for (int i = 0; i < logSize; i++)
            {
                int term;
                string msg;
                string line;
                getline(infile, line);
                istringstream iss(line);
                iss >> term;
                getline(iss, msg);
                if (!msg.empty() && msg[0] == ' ')
                    msg.erase(0, 1);
                log.push_back(LogEntry(term, msg));
            }

            nodes.clear();
            int nodeCount = numPeers - 1;
            for (int i = 0; i < nodeCount; i++)
            {
                int slot_id, availability_zone_id;
                infile >> slot_id >> availability_zone_id;
                nodes.push_back(ReplicaID{slot_id, availability_zone_id});
            }

            sentLength = vector<int>(numPeers, 0);
            ackedLength = vector<int>(numPeers, 0);
            votesReceived.clear();
            currentRole = FOLLOWER;
            currentLeader = nullReplica;

            infile.close();
        }
    }

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
            cout << "Sending VoteResponse: (VoteResponse, " << nodeId.slot_id << ", " << currentTerm << ", true) to node " << cId << endl;
        }
        else
        {
            cout << "Sending VoteResponse: (VoteResponse, " << nodeId.slot_id << ", " << currentTerm << ", false) to node " << cId << endl;
        }
        RaftQuery request;
        request.valid = true;
        request.msg_type = VoteResponse; // Assuming VoteResponse is a valid Message enum value
        request.sender = nodeId;         // This node is the sender of the response
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

        sendRaftQuery(request, cId);
    }

    void onReceiveVoteResponse(ReplicaID &voterId, int term, bool granted)
    {
        if (currentRole == CANDIDATE && term == currentTerm && granted)
        {
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

        // Simulate sending (LogRequest, leaderId, currentTerm, prefixLen,
        // prefixTerm, commitLength, suffix) to followerId
        cout << "Sending LogRequest to follower " << followerId.slot_id << ":\n";
        cout << "  Leader ID     : " << nodeId << "\n";
        cout << "  Term          : " << currentTerm << "\n";
        cout << "  Prefix Length : " << prefixLen << "\n";
        cout << "  Prefix Term   : " << prefixTerm << "\n";
        cout << "  Commit Length : " << commitLength << "\n";
        cout << "  Suffix        : [";
        for (const auto &entry : suffix)
        {
            cout << " (Term: " << entry.term << ", Msg: " << entry.msg << ")";
        }
        cout << " ]\n";

        RaftQuery request;
        request.valid = true;
        request.msg_type = LogRequest; // Assuming LogRequest is a valid Message enum value
        request.sender = nodeId;       // Constructing a ReplicaID from leaderId
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

            cout << "Sending LogResponse to leader " << leaderId << ":\n";
            cout << "  From Node     : " << nodeId.slot_id << "\n";
            cout << "  Term          : " << currentTerm << "\n";
            cout << "  Ack           : " << ack << "\n";
            cout << "  Success       : true\n";

            request.ack = ack;
            request.success = true;
            request.msg_type = LogResponse; // Assuming LogResponse is a valid Message enum value
        }
        else
        {
            cout << "Sending LogResponse to leader " << leaderId << ":\n";
            cout << "  From Node     : " << nodeId.slot_id << "\n";
            cout << "  Term          : " << currentTerm << "\n";
            cout << "  Ack           : 0\n";
            cout << "  Success       : false\n";

            request.ack = 0;
            request.success = false;
            request.msg_type = LogResponse; // Assuming LogResponse is a valid Message enum value
        }

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
                cout << "Delivering to application: " << log[i].msg << endl;
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

    void COMMIT(const string &msg)
    {
        // Simulate delivering a message to the application
        cout << "Committing to application: " << msg << endl;
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

int main()
{
    // ReplicaID self = {0, 999}; // nodeId = (0, 999)
    // vector<ReplicaID> peers = {{1, 100}, {2, 101}, {3, 102}};
    // // Raft raft(self, 4, peers);

    // raft.print();

    // // Sample VoteRequest
    // ReplicaID candidate = {1, 100};
    // raft.onReceiveVoteRequest(candidate, 1, 0, 0); // cTerm=1, logLength=0, logTerm=0

    return 0;
}
