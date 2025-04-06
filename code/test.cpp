#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;

int main()
{
    // Open the JSON file
    ifstream file("nodes.json");
    if (!file.is_open())
    {
        cerr << "Error: Could not open nodes.json" << endl;
        return 1;
    }

    // Parse the JSON file into a json object
    json j;
    try
    {
        file >> j;
    }
    catch (const json::parse_error &e)
    {
        cerr << "Parse error: " << e.what() << endl;
        return 1;
    }

    // Process each node based on its type
    try
    {
        for (const auto &node : j.at("nodes"))
        {
            string type = node.at("type").get<string>();
            cout << "Node type: " << type << endl;

            if (type == "request_manager")
            {
                cout << "  ID: " << node.at("id").get<string>() << endl;
                cout << "  Host: " << node.at("host").get<string>() << endl;
                cout << "  Port: " << node.at("port").get<int>() << endl;
                cout << "  Max Connections: " << node.at("max_connections").get<int>() << endl;
            }
            else if (type == "availability_zone")
            {
                cout << "  ID: " << node.at("id").get<string>() << endl;
                cout << "  Region: " << node.at("region").get<string>() << endl;
                cout << "  Datacenter: " << node.at("datacenter").get<string>() << endl;
                cout << "  Capacity: " << node.at("capacity").get<string>() << endl;
            }
            else if (type == "storage_node")
            {
                cout << "  ID: " << node.at("id").get<string>() << endl;
                cout << "  IP: " << node.at("ip").get<string>() << endl;
                cout << "  Storage Capacity (GB): " << node.at("storage_capacity_gb").get<int>() << endl;
            }
            else
            {
                cout << "  Unknown node type" << endl;
            }
            cout << endl;
        }
    }
    catch (const json::out_of_range &e)
    {
        cerr << "Error accessing JSON data: " << e.what() << endl;
        return 1;
    }
    catch (const exception &e)
    {
        cerr << "General error: " << e.what() << endl;
        return 1;
    }

    return 0;
}
