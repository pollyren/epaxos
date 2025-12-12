#include <grpcpp/grpcpp.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "epaxos.grpc.pb.h"
#include "workload.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace std::chrono;
#include "absl/log/initialize.h"
#include "utils.hpp"

static epaxos::PingResp call_broadcast(const std::shared_ptr<Channel>& ch,
                                     const std::string& msg, int id,
                                     bool fanout) {
    auto stub = epaxos::Echo::NewStub(ch);
    epaxos::PingReq req;
    req.set_msg(msg);
    req.set_id(id);
    req.set_fanout(fanout);
    epaxos::PingResp resp;
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    Status s = stub->Ping(&ctx, req, &resp);
    if (!s.ok())
        throw std::runtime_error("RPC failed with status " +
                                 std::to_string(s.error_code()) + ": " +
                                 s.error_message());
    return resp;
}

static epaxos::WriteResp call_write(const std::shared_ptr<Channel>& ch,
                                  const std::string& key,
                                  const std::string& value) {
    auto stub = epaxos::EPaxosReplica::NewStub(ch);
    epaxos::WriteReq req;
    req.set_key(key);
    req.set_value(value);
    epaxos::WriteResp resp;
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    Status s = stub->ClientWriteReq(&ctx, req, &resp);
    LOG("Response status: " << resp.status() << std::endl);
    if (!s.ok())
        throw std::runtime_error("RPC failed with status " +
                                 std::to_string(s.error_code()) + ": " +
                                 s.error_message());
    return resp;
}

static epaxos::GetStateResp call_get_state(const std::shared_ptr<Channel>& ch) {
    auto stub = epaxos::EPaxosReplica::NewStub(ch);
    epaxos::GetStateReq req;
    epaxos::GetStateResp resp;
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    Status s = stub->ClientGetStateReq(&ctx, req, &resp);
    LOG("Response state: " << resp.state() << std::endl);
    if (!s.ok())
        throw std::runtime_error("RPC failed with status " +
                                 std::to_string(s.error_code()) + ": " +
                                 s.error_message());
    return resp;
}

int run_ep_client(int argc, char** argv) {
    absl::InitializeLog();

    if (argc < 3) {
        std::cerr << "Usage: ./client <mp|e> [args...]\n";
        return 1;
    }

    int expLength;
    int numKeys;
    int zipfS;
    std::string server;

    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--expLength=", 0) == 0)
            expLength = std::stoi(a.substr(12));
        else if (a.rfind("--numKeys=", 0) == 0) {
            numKeys = std::stoi(a.substr(10));
        } else if (a.rfind("--zipfS=", 0) == 0) {
            zipfS = std::stoi((a.substr(8)));
        } else if (a.rfind("--server=", 0) == 0) {
            server = a.substr(9);
        }
    }

    auto expStart = high_resolution_clock::now();
    auto expEnd = expStart + seconds(expLength);
    ZipfGenerator zipf = ZipfGenerator(numKeys, zipfS);
    size_t i = 0;
    std::string key;
    std::string val;

    // create channel to target server
    auto ch = grpc::CreateChannel(server, grpc::InsecureChannelCredentials());

    while (high_resolution_clock::now() < expEnd) {
        // get key using Zipfian Generator
        key = std::to_string(zipf.next());
        val = "val" + std::to_string(i);

        // record time when the operation is initiated
        auto start = high_resolution_clock::now();

        std::string opType = "write";
        LOG("Writing key='" << key << "' value='" << val << "' to server='"
                            << server << "'\n");

        try {
            auto resp = call_write(ch, key, val);
        } catch (const std::exception& e) {
            std::cerr << "error contacting " << server << ": " << e.what()
                      << "\n";
        };

        // record time when request is completed
        auto end = high_resolution_clock::now();

        // calculate request latency
        int64_t latency = duration_cast<nanoseconds>(end - start).count();
        std::cout << opType << "," << latency << "," << key << "," << i << "\n";
        i++;
    }

    return 0;
}
