#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "../build/generated/multipaxos.grpc.pb.h"
#include "absl/log/initialize.h"
#include "mp_types.hpp"
#include "multipaxos.pb.h"
#include "utils.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace std::chrono;

namespace {
std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    std::stringstream ss(s);
    while (std::getline(ss, cur, sep))
        if (!cur.empty()) out.push_back(cur);
    return out;
}

std::string now_ns_str() {
    using namespace std::chrono;
    return std::to_string(
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch())
            .count());
}
}  // namespace

class MultiPaxosReplica final : public mp::MultiPaxosReplica::Service {
   private:
    std::string thisReplica_;  // my name
    bool is_leader_;           // does this replica think it is the leader
    int instanceCounter_ = 0;  // instance counter

    // all peer addrs and stubs
    std::map<std::string, std::unique_ptr<mp::MultiPaxosReplica::Stub>>
        peersNameToStub_;

    int peerSize;
    // majority quorum addrs
    std::vector<std::string> majorityQuorumNames_;

    // instance is a map from key(replica) to a vector of instances
    std::unordered_map<std::string,
                       std::vector<struct multipaxosTypes::Instance>>
        instances;

    // return the string the current state (instances) of this replica
    std::string instances_to_string() {
        std::string res;
        for (const auto& [replica, instVec] : instances) {
            for (const auto& instance : instVec) {
                res += "  - " + printInstance(instance);
            }
        }
        return res;
    }

    std::string printInstance(const multipaxosTypes::Instance& inst) {
        std::ostringstream oss;
        oss << "Instance " << inst.id.replica_id << "."
            << inst.id.replicaInstance_id << " [cmd: action=" << inst.cmd.action
            << " key=" << inst.cmd.key << " value=" << inst.cmd.value
            << "; status="
            << static_cast<int>(inst.status)
            // << "; seq=" << inst.attr.seq
            << "]";
        return oss.str();
    }

    bool accept(const multipaxosTypes::Instance newInstance) {
        // prepare the command (gamma)
        mp::AcceptReq acceptReq;
        mp::Command c;
        c.set_action(mp::Action::WRITE);
        c.set_key(newInstance.cmd.key);
        c.set_value(newInstance.cmd.value);
        acceptReq.mutable_cmd()->CopyFrom(c);

        // prepare id L.i for this instance
        mp::InstanceId id;
        id.set_replica_id(newInstance.id.replica_id);
        id.set_instance_seq_id(newInstance.id.replicaInstance_id);
        acceptReq.mutable_id()->CopyFrom(id);
        acceptReq.set_sender(thisReplica_);

        // mark the instance as accepted locally
        multipaxosTypes::Instance inst = newInstance;
        inst.status = multipaxosTypes::Status::ACCEPTED;
        instances[inst.id.replica_id][inst.id.replicaInstance_id] = inst;

        //  send Accept messages to all majority quorum members
        LOG("----------------------------\n[" << thisReplica_
                  << "] Sending Accept RPCs: " << std::endl);

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            mp::AcceptReply reply;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<mp::AcceptReply>>
                rpc;
        };

        // create a mapping from peer name to its async call
        std::map<std::string, std::unique_ptr<AsyncCall>> calls;

        // now send async Accept RPCs to all majority quorum members
        for (const auto& peerName : majorityQuorumNames_) {
            auto call = std::make_unique<AsyncCall>();

            call->rpc = peersNameToStub_[peerName]->AsyncAccept(&call->ctx,
                                                                acceptReq, &cq);

            // request notification when the operation finishes asynchronously
            call->rpc->Finish(&call->reply, &call->status,
                              (void*)peerName.data());

            // store the call in the map
            calls.emplace(peerName, std::move(call));

            LOG("[" << thisReplica_
                      << "] Sending AcceptReq message to: " << peerName
                      << std::endl);
        }

        // collect all accept replies
        int remaining = majorityQuorumNames_.size();
        std::map<std::string, mp::AcceptReply> acceptReplies;

        while (remaining > 0) {
            void* tag;
            bool ok = false;

            // wait for the next result from the completion queue
            cq.Next(&tag, &ok);

            if (!ok) {
                // RPC stream broken
                throw std::runtime_error("RPC stream error");
            }

            std::string peerName = static_cast<const char*>(tag);
            auto& call = calls[peerName];

            if (!call->status.ok()) {
                throw std::runtime_error("RPC failed from " + peerName + ": " +
                                         call->status.error_message());
            }

            acceptReplies[peerName] = call->reply;
            remaining--;
        }

        LOG("----------------------------\n [" << thisReplica_
                  << "] Accept Reply: " << std::endl);

        int agreeCount = 0;
        for (const auto& [name, reply] : acceptReplies) {
            LOG(" From: " << name << "  Reply Details: "
                      << " ok=" << (reply.ok() ? "true" : "false") << std::endl);

            if (reply.ok()) {
                agreeCount++;
            }
            if (agreeCount >= (peerSize / 2)) {
                break;
            }
        }

        LOG("AgreeCount: " << agreeCount << std::endl);
        LOG("PeerSize/2: " << peerSize / 2 << std::endl);

        // check that we got enough answers
        if (agreeCount >= (peerSize / 2)) {
            LOG("[" << thisReplica_
                      << "] Accept phase succeeded for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << " because agreeCount=" << agreeCount
                      << " >= " << "peerSize/2=" << peerSize / 2
                      << "; Go to Commit phase" << std::endl);

            return true;
        } else {
            // not enough replies, so cancel
            return false;
        }
    }

    bool commit(const multipaxosTypes::Instance newInstance) {
        // mark the instance as committed
        multipaxosTypes::Instance inst = newInstance;
        inst.status = multipaxosTypes::Status::COMMITTED;
        instances[inst.id.replica_id][inst.id.replicaInstance_id] = inst;

        LOG("[" << thisReplica_
                  << "] Committed instance: " << printInstance(inst)
                  << std::endl);
        LOG("[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl);

        // Broadcast commit to all peers
        mp::CommitReq commitReq;

        // prepare id L.i for this instance
        mp::InstanceId id;
        id.set_replica_id(newInstance.id.replica_id);
        id.set_instance_seq_id(newInstance.id.replicaInstance_id);
        commitReq.mutable_id()->CopyFrom(id);
        commitReq.set_sender(thisReplica_);

        // send Commit RPCs asynchronously to all peers
        LOG("----------------------------\n[" << thisReplica_
                  << "] Sending Commit RPCs: " << std::endl);

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            mp::CommitReply reply;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<mp::CommitReply>>
                rpc;
        };

        // create a mapping from peer name to its async call
        std::map<std::string, std::unique_ptr<AsyncCall>> calls;

        // now send async Commit RPCs to all majority quorum members
        for (const auto& [peerName, _] : peersNameToStub_) {
            // allocate an async call object
            auto* call = new AsyncCall;

            // send the commit RPC
            call->rpc = peersNameToStub_[peerName]->AsyncCommit(&call->ctx,
                                                                commitReq, &cq);
            call->rpc->Finish(&call->reply, &call->status, call);

            LOG("[" << thisReplica_
                      << "] Sending CommitReq message to: " << peerName
                      << std::endl);
        }

        // No need to wait for commit replies
        return true;
    }

   public:
    MultiPaxosReplica(
        std::string name, bool is_leader,
        const std::map<std::string, std::string>& peer_name_to_addrs,
        const std::vector<std::string>& majority_quorum_names)
        : thisReplica_(std::move(name)), is_leader_(is_leader) {
        // keep a list of peer addresses and stubs
        for (const auto& [name, addr] : peer_name_to_addrs) {
            peersNameToStub_[name] = mp::MultiPaxosReplica::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }

        for (const auto& a : majority_quorum_names) {
            if (peer_name_to_addrs.find(a) == peer_name_to_addrs.end()) {
                throw std::runtime_error(
                    "Majority quorum name not found in peer list");
            }
            majorityQuorumNames_.push_back(a);
        }

        // Initialize instance map for each replica (peer)
        for (const auto& [name, addr] : peer_name_to_addrs) {
            instances[name] = std::vector<struct multipaxosTypes::Instance>();
        }

        peerSize = peer_name_to_addrs.size();
    }

    Status ClientWriteReq(ServerContext* /*ctx*/, const mp::WriteReq* req,
                          mp::WriteResp* resp) override {
        auto s = high_resolution_clock::now();
        LOG("\n[" << thisReplica_
                  << "] Received ClientWriteReq: key=" << req->key()
                  << " value=" << req->value() << std::endl);

        // Create new instance
        multipaxosTypes::Instance newInstance;
        newInstance.cmd.action = multipaxosTypes::Command::WRITE;
        newInstance.cmd.key = req->key();
        newInstance.cmd.value = req->value();
        newInstance.status = multipaxosTypes::Status::PREPARED;
        newInstance.id.replica_id = thisReplica_;
        newInstance.id.replicaInstance_id = instanceCounter_;
        instanceCounter_++;

        LOG("[" << thisReplica_
                  << "] Created new instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl);

        instances[thisReplica_].push_back(newInstance);

        if (instances[thisReplica_].size() != instanceCounter_) {
            throw std::runtime_error("RPC failed: instance counter mismatch");
        }



        // Accept Phase
        auto accStart = high_resolution_clock::now();
        bool successAccept = accept(newInstance);
        if (!successAccept) {
            return Status::CANCELLED;
        }

        auto accEnd = high_resolution_clock::now();
        // calculate request latency
        int64_t latency = duration_cast<nanoseconds>(accEnd - accStart).count();
        LOG("[" << thisReplica_
                    << "] Accept phase took " << latency
                    << " nanoseconds for instance: "
                    << newInstance.id.replica_id << "."
                    << newInstance.id.replicaInstance_id
                    << std::endl);

        // Commit Phase
        auto commStart = high_resolution_clock::now();
        bool successCommit = commit(newInstance);
        if (!successCommit) {
            return Status::CANCELLED;
        }
        // calculate request latency
        auto commEnd = high_resolution_clock::now();
        latency = duration_cast<nanoseconds>(commEnd - commStart).count();
        LOG("[" << thisReplica_
                    << "] Commit phase took " << latency
                    << " nanoseconds for instance: "
                    << newInstance.id.replica_id << "."
                    << newInstance.id.replicaInstance_id
                    << std::endl);

        // Reply to client
        LOG("[" << thisReplica_
                  << "] Reply to client ClientWriteReq: key=" << req->key()
                  << " value=" << req->value() << " write accepted."
                  << std::endl);
        resp->set_status("write accepted");
        auto e = high_resolution_clock::now();
        latency = duration_cast<nanoseconds>(e-s).count();
        LOG("[" << thisReplica_
                    << "] Entire request took " << latency
                    << " nanoseconds for instance: "
                    << newInstance.id.replica_id << "."
                    << newInstance.id.replicaInstance_id
                    << std::endl);
        return Status::OK;
    }

    Status Accept(ServerContext* /*ctx*/, const mp::AcceptReq* req,
                  mp::AcceptReply* resp) override {
        LOG("----------------------------\n"
                  << "[" << thisReplica_ << "] Received AcceptReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id()
                  << "from sender "
                  << req->sender() << std::endl);

        if (req->sender().empty()) {
            throw std::runtime_error("AcceptReq: replica_id is empty");
        }

        if (req->sender() != req->id().replica_id()) {
            throw std::runtime_error("AcceptReq: sender and proposal mismatch");
        }

        // construct command from request
        multipaxosTypes::Command cmd;
        cmd.action =
            static_cast<multipaxosTypes::Command::Action>(req->cmd().action());
        cmd.key = req->cmd().key();
        cmd.value = req->cmd().value();

        LOG("  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl);

        // construct instance ID from request
        multipaxosTypes::InstanceID instanceId;
        instanceId.replica_id = req->id().replica_id();
        instanceId.replicaInstance_id = req->id().instance_seq_id();

        // store the instance locally
        multipaxosTypes::Instance newInstance;
        newInstance.cmd = cmd;
        newInstance.status = multipaxosTypes::Status::ACCEPTED;
        newInstance.id = instanceId;

        // resize instance vector if needed
        if (instances[instanceId.replica_id].size() <=
            instanceId.replicaInstance_id) {
            instances[instanceId.replica_id].resize(
                instanceId.replicaInstance_id + 1);
        }
        instances[instanceId.replica_id][instanceId.replicaInstance_id] =
            newInstance;

        LOG("[" << thisReplica_
                  << "] Accepted instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl);

        // prepare the reply message
        resp->set_ok(true);
        resp->set_sender(thisReplica_);

        return Status::OK;
    }

    Status Commit(ServerContext* /*ctx*/, const mp::CommitReq* req,
                  mp::CommitReply* resp) override {
        LOG("----------------------------\n"
                  << "[" << thisReplica_ << "] Received CommitReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl);

        if (req->sender().empty()) {
            throw std::runtime_error("CommitReq: replica_id is empty");
        }

        if (req->sender() != req->id().replica_id()) {
            throw std::runtime_error("CommitReq: sender and proposal mismatch");
        }

        // construct command from request
        multipaxosTypes::Command cmd;
        cmd.action =
            static_cast<multipaxosTypes::Command::Action>(req->cmd().action());
        cmd.key = req->cmd().key();
        cmd.value = req->cmd().value();

        LOG("  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl);

        // commit instance
        instances[req->id().replica_id()][req->id().instance_seq_id()].status =
            multipaxosTypes::Status::COMMITTED;

        LOG("[" << thisReplica_ << "] Committed instance: "
                  << printInstance(instances[req->id().replica_id()]
                                            [req->id().instance_seq_id()])
                  << std::endl);
        LOG("[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl);

        return Status::OK;
    }

    Status ClientGetStateReq(ServerContext* /*ctx*/, const mp::GetStateReq* req,
                             mp::GetStateResp* resp) override {
        std::string result = "";
        result += "Instance count: " + std::to_string(instanceCounter_) + "\n";
        resp->set_state(result);
        return Status::OK;
    }
};

class EchoServiceImpl final : public mp::Echo::Service {
   public:
    EchoServiceImpl(std::string name,
                    const std::vector<std::string>& peer_addrs)
        : name_(std::move(name)) {
        // keep a list of peer addresses and stubs
        for (const auto& a : peer_addrs) {
            peer_addrs_.push_back(a);
            peer_stubs_.push_back(mp::Echo::NewStub(
                grpc::CreateChannel(a, grpc::InsecureChannelCredentials())));
        }
    }

    Status Ping(ServerContext* /*ctx*/, const mp::PingReq* req,
                mp::PingResp* resp) override {
        resp->set_reply(std::string("pong: ") + req->msg());
        resp->set_from(name_);

        if (req->fanout()) {
            mp::BcastReq b;
            b.set_msg(req->msg());
            b.set_origin(name_);
            const std::string uuid =
                req->uuid().empty() ? make_uuid() : req->uuid();
            b.set_uuid(uuid);
            b.set_ttl(1);  // set >1 for multi-hop fanout

            auto acks = broadcast_to_peers(b);
            for (const auto& a : acks) resp->add_broadcasted_to(a);
        }
        return Status::OK;
    }

    Status Broadcast(ServerContext* /*ctx*/, const mp::BcastReq* req,
                     mp::BcastResp* resp) override {
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
            mp::BcastReq fwd = *req;  // copy
            fwd.set_ttl(req->ttl() - 1);
            std::thread([this, fwd]() {
                (void)broadcast_to_peers(fwd);
            }).detach();
        }
        return Status::OK;
    }

   private:  // internal state of server
    std::string name_;
    bool is_leader_;
    std::vector<std::string> peer_addrs_;
    std::vector<std::unique_ptr<mp::Echo::Stub>> peer_stubs_;
    std::mutex mu_;
    std::unordered_set<std::string> seen_;  // broadcast de-dup

    std::string make_uuid() const {
        static std::atomic<uint64_t> ctr{0};
        std::stringstream ss;
        ss << name_ << "-" << now_ns_str() << "-" << ++ctr;
        return ss.str();
    }

    std::vector<std::string> broadcast_to_peers(const mp::BcastReq& req) {
        std::vector<std::string> acks;
        for (size_t i = 0; i < peer_stubs_.size(); ++i) {
            mp::BcastResp r;
            grpc::ClientContext ctx;
            auto status = peer_stubs_[i]->Broadcast(&ctx, req, &r);
            if (status.ok()) {
                acks.push_back(peer_addrs_[i] + " <- " + r.ack_from());
            } else {
                acks.push_back(peer_addrs_[i] +
                               " <- ERROR: " + status.error_message());
            }
        }
        return acks;
    }
};

// create a server
int run_mp_server(int argc, char** argv) {
    // Usage: ./server --name=S1 --port=50051
    // --peers=localhost:50052,localhost:50053 --is_leader
    absl::InitializeLog();

    std::string name;
    std::string port = "50051";
    std::string peers_csv;
    bool is_leader = false;
    std::map<std::string, std::string> peer_name_to_addr;
    std::string application = argv[1];  // broadcast or multipaxos

    if (application == "b") {
        for (int i = 2; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--name=", 0) == 0)
                name = a.substr(7);
            else if (a.rfind("--port=", 0) == 0) {
                port = a.substr(7);  // identify its port
                name = a.substr(7);  // use port as UID of server
            } else if (a.rfind("--peers=", 0) == 0)
                peers_csv = a.substr(8);  // identify other servers
        }

        const auto peer_addrs = split(peers_csv, ',');

        EchoServiceImpl service(name, peer_addrs);  // create a server structure

        grpc::ServerBuilder builder;
        const std::string addr = std::string("0.0.0.0:") + port;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());
        LOG("Server listening on " << addr << " peers=" << peers_csv);
        LOG("[" << name << "] listening on " << addr
                  << " peers=" << peers_csv);
        server->Wait();
        return 0;
    } else if (application == "mp") {
        for (int i = 2; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--name=", 0) == 0)
                name = a.substr(7);
            else if (a.rfind("--port=", 0) == 0) {
                port = a.substr(7);  // identify its port
            } else if (a.rfind("--peers=", 0) == 0) {
                peers_csv = a.substr(8);  // identify other servers
            } else if (a.rfind("--peersName2Addr=", 0) == 0) {
                std::string peersName2Addr = a.substr(17);
                peer_name_to_addr = parse_map_mixed_simple(peersName2Addr);
            } else if (a.rfind("--is_leader", 0) == 0) {
                is_leader = true;
            }
        }

        const auto peer_names = split(peers_csv, ',');

        int f = peer_names.size() / 2;
        size_t majority_quorum_size = f + 1;

        LOG("[" << name << "] Determined f=" << f
                  << ", majority_quorum_size=" << majority_quorum_size
                  << std::endl);

        std::vector<std::string> majority_quorum_names(peer_names.begin(), peer_names.begin() + majority_quorum_size - 1);

        MultiPaxosReplica service(
            name, is_leader, peer_name_to_addr, majority_quorum_names);  // create a server structure

        grpc::ServerBuilder builder;
        const std::string addr = std::string("0.0.0.0:") + port;
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());
        if (!server) {
            std::cerr << "[" << name
                      << "] ERROR: failed to start gRPC server on " << addr
                      << " (port may be in use or address invalid)"
                      << std::endl;
            return 1;
        }

        LOG("[" << name << "] listening on MultiPaxos replica " << addr
                  << " peers=" << map_to_string(peer_name_to_addr)
                  << " is_leader=" << std::boolalpha << is_leader << std::endl);
        server->Wait();
        return 0;
    } else {
        std::cerr << "Unknown application type. Use 'b' for broadcast or 'mp' "
                     "for multi-paxos.\n";
        return 1;
    }
}
