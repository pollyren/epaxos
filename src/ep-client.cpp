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

static demo::PingResp call_broadcast(const std::shared_ptr<Channel>& ch,
                                     const std::string& msg, int id,
                                     bool fanout) {
    auto stub = demo::Echo::NewStub(ch);
    demo::PingReq req;
    req.set_msg(msg);
    req.set_id(id);
    req.set_fanout(fanout);
    demo::PingResp resp;
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

static demo::WriteResp call_write(const std::shared_ptr<Channel>& ch,
                                  const std::string& key,
                                  const std::string& value) {
    auto stub = demo::EPaxosReplica::NewStub(ch);
    demo::WriteReq req;
    req.set_key(key);
    req.set_value(value);
    demo::WriteResp resp;
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    Status s = stub->ClientWriteReq(&ctx, req, &resp);
    std::cout << "Response status: " << resp.status() << "\n";
    if (!s.ok())
        throw std::runtime_error("RPC failed with status " +
                                 std::to_string(s.error_code()) + ": " +
                                 s.error_message());
    return resp;
}

static demo::GetStateResp call_get_state(const std::shared_ptr<Channel>& ch) {
    auto stub = demo::EPaxosReplica::NewStub(ch);
    demo::GetStateReq req;
    demo::GetStateResp resp;
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(3));
    Status s = stub->ClientGetStateReq(&ctx, req, &resp);
    std::cout << "Response state: " << resp.state() << "\n";
    if (!s.ok())
        throw std::runtime_error("RPC failed with status " +
                                 std::to_string(s.error_code()) + ": " +
                                 s.error_message());
    return resp;
}

int run_ep_client(int argc, char** argv) {
    absl::InitializeLog();
    
    if (argc < 3) {
        std::cerr << "Usage: ./client <mp|e> <workload_file>\n";
        return 1;
    }

    workload::CSVParser parser;
    auto operations = parser.parse(argv[2]);

    size_t i = 0;
    for (const auto& op : operations) {
        // create channel to target server
        auto ch =
            grpc::CreateChannel(op.server, grpc::InsecureChannelCredentials());

        // record time when the operation is initiated
        auto start = high_resolution_clock::now();
        std::string opType;

        try {
            switch (op.type) {
                case workload::OperationType::OP_WRITE: {
                    opType = "write";
                    std::cout << "Writing key='" << op.key << "' value='"
                              << op.value << "' to server='" << op.server
                              << "'\n";
                    auto resp = call_write(ch, op.key, op.value);
                    break;
                }
                case workload::OperationType::OP_READ: {
                    opType = "read";
                    std::cout << "Read operation not implemented yet.\n";
                    break;
                }
                case workload::OperationType::OP_GET_STATE: {
                    opType = "get_state";
                    std::cout << "Getting state from server='" << op.server
                              << "'\n";
                    auto resp = call_get_state(ch);
                    break;
                }
                case workload::OperationType::OP_BROADCAST: {
                    opType = "broadcast";
                    int id = 1;
                    auto resp = call_broadcast(ch, op.value, id++, true);
                    std::cout << "reply='" << resp.reply()
                              << "' from=" << resp.from();
                    if (resp.broadcasted_to_size() > 0) {
                        std::cout << " | acks:";
                        for (const auto& s : resp.broadcasted_to())
                            std::cout << " [" << s << "]";
                    }
                    std::cout << "\n";
                    break;
                }
            }
        } catch (const std::exception& e) {
            std::cout << "error contacting " << op.server << ": " << e.what()
                      << "\n";
        }

        // record time when request is completed
        auto end = high_resolution_clock::now();

        // calculate request latency
        int64_t latency = duration_cast<nanoseconds>(end - start).count();
        std::cout << opType << "," << latency << "," << op.key << "," << i << "\n";
        i++;
    }

    return 0;
}
