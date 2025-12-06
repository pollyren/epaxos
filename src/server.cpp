#include <iostream>
#include <string>

#include "utils.h"

int run_mp_server(int argc, char** argv);
int run_ep_server(int argc, char** argv);

int main(int argc, char** argv) {
    // Usage: ./server mp --name=S1 --port=50051
    // --peers=localhost:50052,localhost:50053
    // --peer2Addr=...
    // --is_leader=true

    if (argc < 2) {
        std::cerr << "Usage: ./server <mp|e> [args...]\n";
        return 1;
    }

    std::string application = argv[1];  // mp (multi-paxos) or e (epaxos)

    if (application == "mp") {
        std::cout << "Initializing Multi-Paxos Server...\n";
        return run_mp_server(argc, argv);
    } else if (application == "e") {
        std::cout << "Initializing EPaxos Server...\n";
        return run_ep_server(argc, argv);
    } else {
        std::cerr
            << "Unknown application type. Use 'mp' for multi-paxos or 'e' "
               "for epaxos.\n";
        return 1;
    }
}
