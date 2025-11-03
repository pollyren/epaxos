#include <grpcpp/grpcpp.h>
#include "epaxos.grpc.pb.h"
#include "types.hpp" 

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace {
std::vector<std::string> split(const std::string& s, char sep) {
  std::vector<std::string> out; std::string cur; std::stringstream ss(s);
  while (std::getline(ss, cur, sep)) if (!cur.empty()) out.push_back(cur);
  return out;
}

std::string now_ns_str() {
  using namespace std::chrono;
  return std::to_string(duration_cast<nanoseconds>(
    steady_clock::now().time_since_epoch()).count());
}
}


class EPaxosReplica final: public demo::EPaxosReplica::Service{
private: 
  std::string thisReplica_; //my name 

  int instanceCounter_ = 0; //instance counter

  //all peer addrs and stubs
  std::vector<std::string> peer_addrs_;
  std::vector<std::unique_ptr<demo::Echo::Stub>> peer_stubs_;

  //slow quorum addrs
  std::vector<std::string> fastQuorumAddrs_;

  //slow quorum addrs and stubs 
  std::vector<std::string> slowQuorumAddrs_;

  //instance is a map from key(replica) to a vector of instances
  std::unordered_map<std::string , 
                     std::vector<struct epaxosTypes::Instance>> instances;

  std::vector<epaxosTypes::Instance> findDependencies(const epaxosTypes::Command& cmd) {
    std::vector<epaxosTypes::Instance> deps;

    //scan through all instances to find dependencies
    for(const auto& [replica, instVec] : instances) {
      for (const auto& inst : instVec) {
        if (inst.cmd.key == cmd.key) {
          deps.push_back(inst);
        }
      }
    }

    return deps;
  }

  int findMaxSeq(const std::vector<epaxosTypes::Instance>& deps) {
    if(deps.empty()) return 0;

    int maxSeq = -1;
    for (const auto& inst : deps) {
      if (inst.attr.seq > maxSeq) {
        maxSeq = inst.attr.seq;
      }
    }
    return maxSeq;
  }


public: 
  EPaxosReplica(std::string name, 
                const std::vector<std::string>& fast_peer_addrs, 
                const std::vector<std::string>& slow_peer_addrs,
                const std::vector<std::string>& peer_addrs)
      : thisReplica_(std::move(name)) {

    //keep a list of peer addresses and stubs
    for (const auto& a : peer_addrs) {
      peer_addrs_.push_back(a);
      peer_stubs_.push_back(
        demo::Echo::NewStub(grpc::CreateChannel(a, grpc::InsecureChannelCredentials())));
    }

    //initialize a set of fast/slow quorum addresses 
    for (const auto& a : fast_peer_addrs) {
      fastQuorumAddrs_.push_back(a);
    }
    for (const auto& a : slow_peer_addrs) {
      slowQuorumAddrs_.push_back(a);
    }

    //initialize instance map for each replica (peer)
    for (const auto& a : peer_addrs_) {
      instances[a] = std::vector<struct epaxosTypes::Instance>();
    }
  }

    Status ClientWriteReq(ServerContext* /*ctx*/,
              const demo::WriteReq* req,
              demo::WriteResp* resp) override {
        instanceCounter_++;

        epaxosTypes::Instance newInstance;
        newInstance.cmd.action = epaxosTypes::Command::WRITE;
        newInstance.cmd.key = req->key();
        newInstance.cmd.value = req->value();
        newInstance.status = epaxosTypes::Status::PRE_ACCEPTED;

        // add dependencies/Maxsequence  
        auto deps = findDependencies(newInstance.cmd);
        newInstance.attr.deps = deps;
        newInstance.attr.seq = findMaxSeq(deps) + 1;


        //TODO add ballot

        instances[thisReplica_].push_back(newInstance);

        if (instances[thisReplica_].size() != instanceCounter_){
            throw std::runtime_error("RPC failed: instance counter mismatch");
        }
        
        resp->set_status("write accepted");
        return Status::OK;

    }
    Status ClientGetStateReq(ServerContext* /*ctx*/,
              const demo::GetStateReq* req,
              demo::GetStateResp* resp) override {
                
            std::string result = "";
            result += "Instance count: " + std::to_string(instanceCounter_) + "\n";
            resp->set_state(result);
            return Status::OK;
    }  
};

class EchoServiceImpl final : public demo::Echo::Service {
public:
  EchoServiceImpl(std::string name, const std::vector<std::string>& peer_addrs)
      : name_(std::move(name)) {

    //keep a list of peer addresses and stubs
    for (const auto& a : peer_addrs) {
      peer_addrs_.push_back(a);
      peer_stubs_.push_back(
        demo::Echo::NewStub(grpc::CreateChannel(a, grpc::InsecureChannelCredentials())));
    }
  }



  Status Ping(ServerContext* /*ctx*/,
              const demo::PingReq* req,
              demo::PingResp* resp) override {
    resp->set_reply(std::string("pong: ") + req->msg());
    resp->set_from(name_);

    if (req->fanout()) {
      demo::BcastReq b;
      b.set_msg(req->msg());
      b.set_origin(name_);
      const std::string uuid = req->uuid().empty() ? make_uuid() : req->uuid();
      b.set_uuid(uuid);
      b.set_ttl(1); // set >1 for multi-hop fanout

      auto acks = broadcast_to_peers(b);
      for (const auto& a : acks) resp->add_broadcasted_to(a);
    }
    return Status::OK;
  }

  Status Broadcast(ServerContext* /*ctx*/,
                   const demo::BcastReq* req,
                   demo::BcastResp* resp) override {
    // de-dup by uuid
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!seen_.insert(req->uuid()).second) {
        resp->set_ack_from(name_ + " (dup)");
        return Status::OK;
      }
    }

    resp->set_ack_from(name_);

    // forward if ttl > 0 (fire-and-forget to avoid blocking caller)
    if (req->ttl() > 0) {
      demo::BcastReq fwd = *req; // copy
      fwd.set_ttl(req->ttl() - 1);
      std::thread([this, fwd]() {
        (void)broadcast_to_peers(fwd);
      }).detach();
    }
    return Status::OK;
  }

private://internal state of server
  std::string name_;
  std::vector<std::string> peer_addrs_;
  std::vector<std::unique_ptr<demo::Echo::Stub>> peer_stubs_;
  std::mutex mu_;
  std::unordered_set<std::string> seen_; // broadcast de-dup
  
  std::string make_uuid() const {
    static std::atomic<uint64_t> ctr{0};
    std::stringstream ss; ss << name_ << "-" << now_ns_str() << "-" << ++ctr;
    return ss.str();
  }

  std::vector<std::string> broadcast_to_peers(const demo::BcastReq& req) {
    std::vector<std::string> acks;
    for (size_t i = 0; i < peer_stubs_.size(); ++i) {
      demo::BcastResp r; grpc::ClientContext ctx;
      auto status = peer_stubs_[i]->Broadcast(&ctx, req, &r);
      if (status.ok()) {
        acks.push_back(peer_addrs_[i] + " <- " + r.ack_from());
      } else {
        acks.push_back(peer_addrs_[i] + " <- ERROR: " + status.error_message());
      }
    }
    return acks;
  }

};


//create a server
int main(int argc, char** argv) {


  // Usage: ./server --name=S1 --port=50051 --peers=localhost:50052,localhost:50053
  std::string name = "S?";
  std::string port = "50051";
  std::string peers_csv;

  std::string application = argv[1]; //broadcast or epaxos 

  if(application == "b"){
        for (int i = 2; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--name=", 0) == 0) name = a.substr(7);
            else if (a.rfind("--port=", 0) == 0) port = a.substr(7);       // identify its port
            else if (a.rfind("--peers=", 0) == 0) peers_csv = a.substr(8); // identify other servers
        }

        const auto peer_addrs = split(peers_csv, ',');
        EchoServiceImpl service(name, peer_addrs); //create a server structure

        grpc::ServerBuilder builder;
        const std::string addr = std::string("0.0.0.0:") + port;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "[" << name << "] listening on " << addr
                    << " peers=" << peers_csv << std::endl;
        server->Wait();
        return 0;
  } else if (application == "e"){
            for (int i = 2; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--name=", 0) == 0) name = a.substr(7);
            else if (a.rfind("--port=", 0) == 0) port = a.substr(7);       // identify its port
            else if (a.rfind("--peers=", 0) == 0) peers_csv = a.substr(8); // identify other servers
        }

        const auto peer_addrs = split(peers_csv, ',');
        
        EPaxosReplica service(name, peer_addrs, peer_addrs,peer_addrs); //create a server structure

        grpc::ServerBuilder builder;
        const std::string addr = std::string("0.0.0.0:") + port;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "[" << name << "] listening on EPaxos replica" << addr
                    << " peers=" << peers_csv << std::endl;
        server->Wait();
        return 0;
  } else {
    std::cerr << "Unknown application type. Use 'b' for broadcast or 'e' for epaxos.\n";
    return 1;
  }


}
