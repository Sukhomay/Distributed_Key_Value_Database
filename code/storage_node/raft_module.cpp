#include <vector>
#include <set>
#include <optional>
#include <string>
using namespace std;
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
    struct LogEntry
    {
        int term;
        string msg;

        LogEntry(int t, const string &m) : term(t), msg(m) {}
    };
    struct Follower
    {
        int replicaID;
        int AZ_ID;
    };
    // Constructor
    Raft(int numPeers, vector<Follower> &followers) : currentTerm(0),
                                                      votedFor(nullopt),
                                                      commitLength(0),
                                                      currentRole(FOLLOWER),
                                                      currentLeader(nullopt),
                                                      votesReceived(),
                                                      sentLength(numPeers, 0),
                                                      ackedLength(numPeers, 0),
                                                      nodes(followers)
    {
        initState();
    }

private:
    int currentTerm;
    optional<int> votedFor;
    vector<LogEntry> log;
    int commitLength;
    Role currentRole;
    optional<int> currentLeader;
    set<int> votesReceived;
    vector<int> sentLength;
    vector<int> ackedLength;
    vector<Follower> nodes;

    void initState(){
        
    }
};

