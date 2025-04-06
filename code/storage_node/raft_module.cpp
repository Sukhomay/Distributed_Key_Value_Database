#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <cstdlib>
using namespace std;

#define nullValue -1

class Raft
{
public:
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
        DEL
    };
    enum Message
    {
        VoteRequest,
        VoteResponse,
        LogRequest,
        LogResponse
    };

    typedef struct LogEntry
    {
        int term;
        string msg;
        LogEntry(int t, const string &m) : term(t), msg(m) {}
    } LogEntry;

    typedef struct Peer
    {
        int replicaID;
        int AZ_ID;
    } Peer;

    Raft(Peer self, int numPeers, vector<Peer> &peers)
        : nodeId(self),
          numPeers(numPeers),
          currentTerm(0),
          votedFor(nullValue),
          commitLength(0),
          currentRole(FOLLOWER),
          currentLeader(nullValue),
          votesReceived(),
          sentLength(numPeers, 0),
          ackedLength(numPeers, 0),
          nodes(peers)
    {
        initState();
    }

    void print() const
    {
        cout << "======== Raft State ========" << endl;
        cout << "nodeId         : (" << nodeId.replicaID << ", " << nodeId.AZ_ID << ")" << endl;
        cout << "numPeers       : " << numPeers << endl;
        cout << "currentTerm    : " << currentTerm << endl;
        cout << "votedFor       : " << votedFor << endl;
        cout << "commitLength   : " << commitLength << endl;
        cout << "currentRole    : " << roleToString(currentRole) << endl;
        cout << "currentLeader  : " << currentLeader << endl;

        cout << "votesReceived  : { ";
        for (int vote : votesReceived)
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
            cout << "  Node " << i << " -> replicaID: " << nodes[i].replicaID
                 << ", AZ_ID: " << nodes[i].AZ_ID << endl;
        }
        cout << "============================" << endl;
    }

private:
    Peer nodeId;
    int numPeers;
    int currentTerm;
    int votedFor;
    vector<LogEntry> log;
    int commitLength;
    Role currentRole;
    int currentLeader;
    set<int> votesReceived;
    vector<int> sentLength;
    vector<int> ackedLength;
    vector<Peer> nodes;
    thread periodicThread;
    mutex mtx;
    chrono::steady_clock::time_point electionDeadline;
    int randomElectionTimeout()
    {
        return 300 + (rand() % 201); // 300 to 500 ms
    }

    void writeToStableStorage()
    {
        ofstream outfile("raft_file.txt");
        if (!outfile.is_open())
        {
            cerr << "Error opening raft_file.txt for writing!" << endl;
            return;
        }

        // Write nodeId
        outfile << nodeId.replicaID << " " << nodeId.AZ_ID << endl;

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
        {
            outfile << f.replicaID << " " << f.AZ_ID << endl;
        }

        outfile.close();
    }

    void runPeriodicTask() {
        resetElectionTimer();
        while (true) {
            writeToStableStorage();
            this_thread::sleep_for(chrono::milliseconds(100)); // wakes up periodically
    
            lock_guard<mutex> lock(mtx);
            if (currentRole == LEADER) {
                for (const Peer &follower : nodes) {
                    if (follower.replicaID != nodeId.replicaID) {
                        replicateLog(nodeId.replicaID, follower);
                    }
                }
            } else {
                if (chrono::steady_clock::now() >= electionDeadline) {
                    currentTerm += 1;
                    currentRole = CANDIDATE;
                    votedFor = nodeId.replicaID;
                    votesReceived.clear();
                    votesReceived.insert(nodeId.replicaID);
                    
                    int lastTerm = 0;
                    if (!log.empty())
                        lastTerm = log.back().term;
    
                    cout << "Election timeout: starting election for term " << currentTerm << endl;
                    cout << "Sending VoteRequest: (VoteRequest, " << nodeId.replicaID << ", " 
                         << currentTerm << ", " << log.size() << ", " << lastTerm << ")" << endl;
                    for (const Peer &peer : nodes) {
                        cout << "   Sending VoteRequest to node " << peer.replicaID << endl;
                    }
                    resetElectionTimer();
                }
            }
        }
    }

    void resetElectionTimer() {
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
        ifstream infile("raft_file.txt");
        if (infile.good())
        {
            infile >> nodeId.replicaID >> nodeId.AZ_ID;
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
                int replicaID, AZ_ID;
                infile >> replicaID >> AZ_ID;
                nodes.push_back(Peer{replicaID, AZ_ID});
            }

            sentLength = vector<int>(numPeers, 0);
            ackedLength = vector<int>(numPeers, 0);
            votesReceived.clear();
            currentRole = FOLLOWER;
            currentLeader = nullValue;

            infile.close();
        }
    }

    void onReceiveVoteRequest(int cId, int cTerm, int cLogLength, int cLogTerm)
    {
        if (cTerm > currentTerm)
        {
            currentTerm = cTerm;
            currentRole = FOLLOWER;
            votedFor = nullValue;
        }

        int lastTerm = 0;
        if (!log.empty())
            lastTerm = log.back().term;

        bool logOk = (cLogTerm > lastTerm) || (cLogTerm == lastTerm && cLogLength >= static_cast<int>(log.size()));

        if (cTerm == currentTerm && logOk && (votedFor == cId || votedFor == nullValue))
        {
            votedFor = cId;
            cout << "Sending VoteResponse: (VoteResponse, " << nodeId.replicaID << ", " << currentTerm << ", true) to node " << cId << endl;
        }
        else
        {
            cout << "Sending VoteResponse: (VoteResponse, " << nodeId.replicaID << ", " << currentTerm << ", false) to node " << cId << endl;
        }
    }

    void onReceiveVoteResponse(int voterId, int term, bool granted)
    {
        if (currentRole == CANDIDATE && term == currentTerm && granted)
        {
            votesReceived.insert(voterId);
            if ((votesReceived.size() + 1) >= (numPeers / 2 + 1))
            {
                currentRole = LEADER;
                currentLeader = nodeId.replicaID;
                resetElectionTimer();
                for (size_t i = 0; i < nodes.size(); i++)
                {
                    sentLength[i] = static_cast<int>(log.size());
                    ackedLength[i] = 0;
                    replicateLog(nodeId.replicaID, nodes[i]);
                }
            }
        }
        else if (term > currentTerm)
        {
            currentTerm = term;
            currentRole = FOLLOWER;
            votedFor = nullValue;
            resetElectionTimer();
        }
    }

    void replicateLog(int leaderId, const Peer &followerId)
    {
        int prefixLen = sentLength[followerId.replicaID];
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
        cout << "Sending LogRequest to follower " << followerId.replicaID << ":\n";
        cout << "  Leader ID     : " << leaderId << "\n";
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
    }

    void onReceiveLogRequest(int leaderId, int term, int prefixLen, int prefixTerm,
                             int leaderCommit, const vector<LogEntry> &suffix)
    {
        if (term > currentTerm)
        {
            currentTerm = term;
            votedFor = nullValue;
            resetElectionTimer();
        }

        if (term == currentTerm)
        {
            currentRole = FOLLOWER;
            currentLeader = leaderId;
        }

        bool logOk = (log.size() >= static_cast<size_t>(prefixLen)) &&
                     (prefixLen == 0 || log[prefixLen - 1].term == prefixTerm);

        if (term == currentTerm && logOk)
        {
            appendEntries(prefixLen, leaderCommit, suffix);
            int ack = prefixLen + suffix.size();

            // Simulated sending of LogResponse
            cout << "Sending LogResponse to leader " << leaderId << ":\n";
            cout << "  From Node     : " << nodeId.replicaID << "\n";
            cout << "  Term          : " << currentTerm << "\n";
            cout << "  Ack           : " << ack << "\n";
            cout << "  Success       : true\n";
        }
        else
        {
            // Simulated sending of failed LogResponse
            cout << "Sending LogResponse to leader " << leaderId << ":\n";
            cout << "  From Node     : " << nodeId.replicaID << "\n";
            cout << "  Term          : " << currentTerm << "\n";
            cout << "  Ack           : 0\n";
            cout << "  Success       : false\n";
        }
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

    void onReceiveLogResponse(int followerId, int term, int ack, bool success)
    {
        if (term == currentTerm && currentRole == LEADER)
        {
            if (success && ack >= ackedLength[followerId])
            {
                sentLength[followerId] = ack;
                ackedLength[followerId] = ack;
                commitLogEntries(); // This function must be implemented separately
            }
            else if (sentLength[followerId] > 0)
            {
                sentLength[followerId]--;
                replicateLog(nodeId.replicaID, followerId); // Re-send with decremented prefix
            }
        }
        else if (term > currentTerm)
        {
            currentTerm = term;
            currentRole = FOLLOWER;
            votedFor = nullValue;
            resetElectionTimer();
        }
    }

    void COMMIT(const string &msg)
    {
        // Simulate delivering a message to the application
        cout << "Committing to application: " << msg << endl;
    }

    void broadcastMessage(const std::string& msg) {
        std::lock_guard<std::mutex> lock(mtx);
    
        if (currentRole == LEADER) {
            // Append the record to log
            log.emplace_back(currentTerm, msg);
    
            // Acknowledge for self
            ackedLength[nodeId.replicaID] = log.size();
    
            // Replicate to all followers
            for (const Peer& follower : nodes) {
                if (follower.replicaID != nodeId.replicaID) {
                    replicateLog(nodeId.replicaID, follower);
                }
            }
        } else {
            // Forward to leader via FIFO (placeholder for actual networking)
            std::cout << "[Forwarding to Leader] "
                      << "LeaderID: " << currentLeader
                      << " | Message: " << msg << std::endl;
    
            // TODO: Implement actual message forwarding over network
        }
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
    Raft::Peer self = {0, 999}; // nodeId = (0, 999)
    vector<Raft::Peer> peers = {{1, 100}, {2, 101}, {3, 102}};
    Raft raft(self, 4, peers);

    raft.print();

    // Sample VoteRequest
    Raft::Peer candidate = {1, 100};
    raft.onReceiveVoteRequest(candidate, 1, 0, 0); // cTerm=1, logLength=0, logTerm=0

    return 0;
}
