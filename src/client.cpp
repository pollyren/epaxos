#include <iostream>
#include <string>

int run_mp_client(int argc, char** argv);
int run_ep_client(int argc, char** argv);

int main(int argc, char** argv) {
    // Usage: ./client mp <workload-file>

    if (argc < 3) {
        std::cerr << "Usage: ./server <mp|e> <workload_file>\n";
        return 1;
    }

    std::string application = argv[1];  // mp (multi-paxos) or e (epaxos)

    if (application == "mp") {
        std::cerr << "Initializing Multi-Paxos Client...\n";
        return run_mp_client(argc, argv);
    } else if (application == "e") {
        std::cerr << "Initializing EPaxos Client...\n";
        return run_ep_client(argc, argv);
    } else {
        std::cerr
            << "Unknown application type. Use 'mp' for multi-paxos or 'e' "
               "for epaxos.\n";
        return 1;
    }
}
