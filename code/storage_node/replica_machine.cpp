#include <iostream>
#include <stdexcept>
#include <string>
#include <cstring>
#include <cstdlib>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "lsm.h" // External functions: start_compaction(), SET(), GET(), DEL()

using namespace std;

#define MAXLINE 1024

class ReplicaMachine
{
public:
    ReplicaMachine(const string &replica_id)
        : replica_id(replica_id)
    {
    }

    ~ReplicaMachine() {}

    // Start the server loop
    ReturnStatus start()
    {
        try
        {
            cout << "Replica Machine " << replica_id << " running..." << endl;
            return run();
        }
        catch (const exception &e)
        {
            cerr << "Replica " << replica_id << ":: Exception in start(): " << e.what() << endl;
            throw;
        }
    }

private:
    string replica_id;

    // Main event loop (to be implemented)
    ReturnStatus run()
    {
        try
        {
            MergeTree mt(replica_id);
            while (true)
            {

            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
            throw;
            return ReturnStatus::FAILURE;
        }
        return ReturnStatus::SUCCESS;
    }
};

// -------------------------------------------------------------------------
// Driver Function
int main(int argc, char *argv[])
{
    try
    {
        if (argc < 2)
        {
            cerr << "Error at ReplicaMachine: replica_id is empty" << endl;
            return ReturnStatus::FAILURE;
        }
        ReplicaMachine replica(argv[1]);
        replica.start();
    }
    catch (const std::exception &e)
    {
        cerr << "ReplicaMachine terminated with error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
// -------------------------------------------------------------------------
