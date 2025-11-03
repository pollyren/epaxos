#include <grpcpp/grpcpp.h>
#include "epaxos.grpc.pb.h"

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
int a;

static demo::PingResp do_call(const std::shared_ptr<Channel>& ch,
                              const std::string& msg, int id, bool fanout) {
  auto stub = demo::Echo::NewStub(ch);
  demo::PingReq req; req.set_msg(msg); req.set_id(id); req.set_fanout(fanout);
  demo::PingResp resp; ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
  Status s = stub->Ping(&ctx, req, &resp);
  if (!s.ok()) throw std::runtime_error("RPC failed: " + s.error_message());
  return resp;
}

static demo::WriteResp call_write(const std::shared_ptr<Channel>& ch,
                             const std::string& key, const std::string& value) {

  auto stub = demo::EPaxosReplica::NewStub(ch);
  demo::WriteReq req; req.set_key(key); req.set_value(value);
  demo::WriteResp resp; ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    Status s = stub->ClientWriteReq(&ctx, req, &resp);
    std::cout << "Response status: " << resp.status() << "\n";
    if (!s.ok()) throw std::runtime_error("RPC failed: " + s.error_message());
    return resp;
}

 
int main(int argc, char** argv) {
  // Usage:
  //   ./client [--fanout] <host:port> [<host:port> ...] <message>
  if (argc < 3) {
    std::cerr << "Usage: ./client [--fanout] <host:port> [<host:port> ...] <message>\n";
    return 1;
  }

  int argi = 1;


  char type = argv[argi][0];
  ++argi;

  switch (type) {
    case 'w':{
        std::cout << "send request to epaxos replica(s)\n";
        if (argc - argi < 2) { std::cerr << "missing args\n"; return 1; }
        std::string key = argv[argi++];
        std::string value = argv[argi++];
        std::vector<std::string> addrs;
        for (int i = argi; i < argc; ++i) addrs.emplace_back(argv[i]);


        int id = 1;
        for (const auto& a : addrs) {
            auto ch = grpc::CreateChannel(a, grpc::InsecureChannelCredentials());
            try {
            auto resp = call_write(ch, key, value);
            
            std::cout << "\n";
            } catch (const std::exception& e) {
            std::cout << "error contacting " << a << ": " << e.what() << "\n";
            }

        }
    }

    break;
    case 'b':{
        // Broadcast
        std::cout << "send request to Ping replica(s)\n";
        bool fanout = false; 
        if (std::string(argv[argi]) == "--fanout") { fanout = true; ++argi; }


        if (argc - argi < 2) { std::cerr << "missing args\n"; return 1; }

        std::vector<std::string> addrs;
        for (int i = argi; i < argc - 1; ++i) addrs.emplace_back(argv[i]);
        std::string msg = argv[argc - 1];

        int id = 1;
        for (const auto& a : addrs) {
            auto ch = grpc::CreateChannel(a, grpc::InsecureChannelCredentials());
            try {
            auto resp = do_call(ch, msg, id++, fanout);
            std::cout << "reply='" << resp.reply() << "' from=" << resp.from();
            if (resp.broadcasted_to_size() > 0) {
                std::cout << " | acks:";
                for (const auto& s : resp.broadcasted_to()) std::cout << " [" << s << "]";
            }
            std::cout << "\n";
            } catch (const std::exception& e) {
            std::cout << "error contacting " << a << ": " << e.what() << "\n";
            }
        }
    }
  }

  return 0;
}
