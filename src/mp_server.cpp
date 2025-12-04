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
#include "mp_types.hpp"
#include "multipaxos.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

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

    bool prepare(const multipaxosTypes::Instance newInstance) {
        // create prepare request
        mp::PrepareReq prepareReq;

        // prepare the command (gamma)
        mp::Command c;
        c.set_action(mp::Action::WRITE);
        c.set_key(newInstance.cmd.key);
        c.set_value(newInstance.cmd.value);
        prepareReq.mutable_cmd()->CopyFrom(c);

        // prepare id L.i for this instance
        mp::InstanceId id;
        id.set_replica_id(newInstance.id.replica_id);
        id.set_instance_seq_id(newInstance.id.replicaInstance_id);
        prepareReq.mutable_id()->CopyFrom(id);
        prepareReq.set_sender(thisReplica_);

        //  send Prepare messages to all majority quorum members
        std::cout << "----------------------------\n[" << thisReplica_
                  << "] Sending Prepare RPCs: " << std::endl;

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            mp::PrepareReply reply;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<mp::PrepareReply>>
                rpc;
        };

        // create a mapping from peer name to its async call
        std::map<std::string, std::unique_ptr<AsyncCall>> calls;

        // now send async Prepare RPCs to all majority quorum members
        for (const auto& peerName : majorityQuorumNames_) {
            auto call = std::make_unique<AsyncCall>();

            call->rpc = peersNameToStub_[peerName]->AsyncPrepare(
                &call->ctx, prepareReq, &cq);

            // request notification when the operation finishes asynchronously
            call->rpc->Finish(&call->reply, &call->status,
                              (void*)peerName.data());

            // store the call in the map
            calls.emplace(peerName, std::move(call));

            std::cout << "  Sent Prepare RPC to " << peerName << std::endl;
        }

        // collect all prepare replies
        int remaining = majorityQuorumNames_.size();
        std::map<std::string, mp::PrepareReply> prepareReplies;

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

            prepareReplies[peerName] = call->reply;
            remaining--;
        }

        std::cout << "----------------------------\n [" << thisReplica_
                  << "] Prepare Reply: " << std::endl;

        int agreeCount = 0;
        for (const auto& [name, reply] : prepareReplies) {
            std::cout << " From: " << name << "  Reply Details: "
                      << " ok=" << (reply.ok() ? "true" : "false") << std::endl;

            if (reply.ok()) {
                agreeCount++;
            }
            if (agreeCount >= (peerSize / 2)) {
                break;
            }
        }

        std::cout << "AgreeCount: " << agreeCount << std::endl;
        std::cout << "PeerSize/2: " << peerSize / 2 << std::endl;

        // check that we got enough answers
        if (agreeCount >= (peerSize / 2)) {
            std::cout << "[" << thisReplica_
                      << "] Prepare phase succeeded for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << " because agreeCount=" << agreeCount
                      << " >= " << "peerSize/2=" << peerSize / 2
                      << "; Go to Accept phase" << std::endl;

            return true;
        } else {
            // not enough replies, so cancel
            return false;
        }
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
        std::cout << "----------------------------\n[" << thisReplica_
                  << "] Sending Accept RPCs: " << std::endl;

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

            std::cout << "[" << thisReplica_
                      << "] Sending AcceptReq message to: " << peerName
                      << std::endl;
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

        std::cout << "----------------------------\n [" << thisReplica_
                  << "] Accept Reply: " << std::endl;

        int agreeCount = 0;
        for (const auto& [name, reply] : acceptReplies) {
            std::cout << " From: " << name << "  Reply Details: "
                      << " ok=" << (reply.ok() ? "true" : "false") << std::endl;

            if (reply.ok()) {
                agreeCount++;
            }
            if (agreeCount >= (peerSize / 2)) {
                break;
            }
        }

        std::cout << "AgreeCount: " << agreeCount << std::endl;
        std::cout << "PeerSize/2: " << peerSize / 2 << std::endl;

        // check that we got enough answers
        if (agreeCount >= (peerSize / 2)) {
            std::cout << "[" << thisReplica_
                      << "] Accept phase succeeded for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << " because agreeCount=" << agreeCount
                      << " >= " << "peerSize/2=" << peerSize / 2
                      << "; Go to Commit phase" << std::endl;

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

        std::cout << "[" << thisReplica_
                  << "] Committed instance: " << printInstance(inst)
                  << std::endl;
        std::cout << "[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl;

        // Broadcast commit to majority quorum members
        mp::CommitReq commitReq;

        // prepare id L.i for this instance
        mp::InstanceId id;
        id.set_replica_id(newInstance.id.replica_id);
        id.set_instance_seq_id(newInstance.id.replicaInstance_id);
        commitReq.mutable_id()->CopyFrom(id);
        commitReq.set_sender(thisReplica_);

        // send Commit RPCs asynchronously to all majority quorum members
        std::cout << "----------------------------\n[" << thisReplica_
                  << "] Sending Commit RPCs: " << std::endl;

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

        // now send async Accept RPCs to all majority quorum members
        for (const auto& peerName : majorityQuorumNames_) {
            // allocate an async call object
            auto* call = new AsyncCall;

            // send the commit RPC
            call->rpc = peersNameToStub_[peerName]->AsyncCommit(&call->ctx,
                                                                commitReq, &cq);
            call->rpc->Finish(&call->reply, &call->status, call);

            std::cout << "[" << thisReplica_
                      << "] Sending CommitReq message to: " << peerName
                      << std::endl;
        }

        // No need to wait for commit replies
        return true;
    }

   public:
    MultiPaxosReplica(
        std::string name, bool is_leader,
        const std::map<std::string, std::string>& peer_name_to_addrs)
        : thisReplica_(std::move(name)), is_leader_(is_leader) {
        // keep a list of peer addresses and stubs
        for (const auto& [name, addr] : peer_name_to_addrs) {
            peersNameToStub_[name] = mp::MultiPaxosReplica::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }

        // Compute majority size: n/2 + 1 (including itself)
        const size_t n = peersNameToStub_.size();
        const size_t majoritySize = n / 2;

        // Initialize the majority quorum
        size_t count = 0;
        for (const auto& [peerName, _] : peersNameToStub_) {
            if (count >= majoritySize) break;
            majorityQuorumNames_.push_back(peerName);
            ++count;
        }

        // Initialize instance map for each replica (peer)
        for (const auto& [name, addr] : peer_name_to_addrs) {
            instances[name] = std::vector<struct multipaxosTypes::Instance>();
        }

        peerSize = peer_name_to_addrs.size();
    }

    Status ClientWriteReq(ServerContext* /*ctx*/, const mp::WriteReq* req,
                          mp::WriteResp* resp) override {
        std::cout << "\n[" << thisReplica_
                  << "] Received ClientWriteReq: key=" << req->key()
                  << " value=" << req->value() << std::endl;

        // Create new instance
        multipaxosTypes::Instance newInstance;
        newInstance.cmd.action = multipaxosTypes::Command::WRITE;
        newInstance.cmd.key = req->key();
        newInstance.cmd.value = req->value();
        newInstance.status = multipaxosTypes::Status::PREPARED;
        newInstance.id.replica_id = thisReplica_;
        newInstance.id.replicaInstance_id = instanceCounter_;
        instanceCounter_++;

        std::cout << "[" << thisReplica_
                  << "] Created new instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl;

        instances[thisReplica_].push_back(newInstance);

        if (instances[thisReplica_].size() != instanceCounter_) {
            throw std::runtime_error("RPC failed: instance counter mismatch");
        }

        // Prepare Phase
        bool successPrepare = prepare(newInstance);
        if (!successPrepare) {
            return Status::CANCELLED;
        }

        // Accept Phase
        bool successAccept = accept(newInstance);
        if (!successAccept) {
            return Status::CANCELLED;
        }

        // Commit Phase
        bool successCommit = commit(newInstance);
        if (!successCommit) {
            return Status::CANCELLED;
        }

        // Reply to client
        std::cout << "[" << thisReplica_
                  << "] Reply to client ClientWriteReq: key=" << req->key()
                  << " value=" << req->value() << " write accepted."
                  << std::endl;
        resp->set_status("write accepted");
        return Status::OK;
    }

    Status Prepare(ServerContext* /*ctx*/, const mp::PrepareReq* req,
                   mp::PrepareReply* resp) override {
        std::cout << "----------------------------\n"
                  << "[" << thisReplica_
                  << "] Received PrepareReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl;

        if (req->sender().empty()) {
            throw std::runtime_error("PrepareReq: replica_id is empty");
        }

        if (req->sender() != req->id().replica_id()) {
            throw std::runtime_error(
                "PrepareReq: sender and proposal mismatch");
        }

        // construct command from request
        multipaxosTypes::Command cmd;
        cmd.action =
            static_cast<multipaxosTypes::Command::Action>(req->cmd().action());
        cmd.key = req->cmd().key();
        cmd.value = req->cmd().value();

        std::cout << "  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl;

        // construct instance ID from request
        multipaxosTypes::InstanceID instanceId;
        instanceId.replica_id = req->id().replica_id();
        instanceId.replicaInstance_id = req->id().instance_seq_id();

        // store the instance locally
        multipaxosTypes::Instance newInstance;
        newInstance.cmd = cmd;
        newInstance.status = multipaxosTypes::Status::PREPARED;
        newInstance.id = instanceId;

        // resize instance vector if needed
        if (instances[instanceId.replica_id].size() <=
            instanceId.replicaInstance_id) {
            instances[instanceId.replica_id].resize(
                instanceId.replicaInstance_id + 1);
        }
        instances[instanceId.replica_id][instanceId.replicaInstance_id] =
            newInstance;

        std::cout << "[" << thisReplica_
                  << "] Stored new instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl;

        // prepare the reply message
        resp->set_ok(true);
        resp->set_sender(thisReplica_);

        return Status::OK;
    }

    Status Accept(ServerContext* /*ctx*/, const mp::AcceptReq* req,
                  mp::AcceptReply* resp) override {
        std::cout << "----------------------------\n"
                  << "[" << thisReplica_ << "] Received AcceptReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl;

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

        std::cout << "  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl;

        // get instance ID from request
        multipaxosTypes::InstanceID instanceId;
        instanceId.replica_id = req->id().replica_id();
        instanceId.replicaInstance_id = req->id().instance_seq_id();

        // update the instance status locally
        instances[instanceId.replica_id][instanceId.replicaInstance_id].status =
            multipaxosTypes::Status::ACCEPTED;

        std::cout << "[" << thisReplica_
                  << "] Accepted instance: " << instanceId.replica_id << "."
                  << instanceId.replicaInstance_id << std::endl;

        // prepare the reply message
        resp->set_ok(true);
        resp->set_sender(thisReplica_);

        return Status::OK;
    }

    Status Commit(ServerContext* /*ctx*/, const mp::CommitReq* req,
                  mp::CommitReply* resp) override {
        std::cout << "----------------------------\n"
                  << "[" << thisReplica_ << "] Received CommitReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl;

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

        std::cout << "  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl;

        // commit instance
        instances[req->id().replica_id()][req->id().instance_seq_id()].status =
            multipaxosTypes::Status::COMMITTED;

        std::cout << "[" << thisReplica_ << "] Committed instance: "
                  << printInstance(instances[req->id().replica_id()]
                                            [req->id().instance_seq_id()])
                  << std::endl;
        std::cout << "[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl;

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

// helper functions for parsing

static inline void trim(std::string& t) {
    auto a = std::find_if_not(t.begin(), t.end(), ::isspace);
    auto b = std::find_if_not(t.rbegin(), t.rend(), ::isspace).base();
    t = (a < b) ? std::string(a, b) : std::string();
}

std::map<std::string, std::string> parse_map_mixed_simple(
    const std::string& s) {
    std::map<std::string, std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        trim(tok);
        // try -+>
        size_t gt = tok.find_last_of('>');
        size_t sep_start = std::string::npos, sep_end = std::string::npos;
        if (gt != std::string::npos && gt > 0) {
            size_t d = gt;
            while (d > 0 && tok[d - 1] == '-') --d;
            if (d < gt) {
                sep_start = d;
                sep_end = gt + 1;
            }
        }
        // otherwise try run of '='
        if (sep_start == std::string::npos) {
            size_t i = tok.find('=');
            if (i != std::string::npos) {
                size_t j = i;
                while (j < tok.size() && tok[j] == '=') ++j;
                sep_start = i;
                sep_end = j;
            }
        }
        if (sep_start == std::string::npos) continue;
        std::string key = tok.substr(0, sep_start);
        std::string val = tok.substr(sep_end);
        trim(key);
        trim(val);
        if (!key.empty() && !val.empty()) out[key] = val;
    }
    return out;
}

template <class Map>
std::string map_to_string(const Map& m, const std::string& arrow = "-->",
                          const std::string& sep = ",  ") {
    std::string out;
    bool first = true;
    for (const auto& kv : m) {
        if (!first) out += sep;
        out += kv.first;
        out += arrow;
        out += kv.second;
        first = false;
    }
    return out;
}

// create a server
int main(int argc, char** argv) {
    // Usage: ./server --name=S1 --port=50051
    // --peers=localhost:50052,localhost:50053 --is_leader
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
        std::cout << "Server listening on " << addr << " peers=" << peers_csv
                  << std::endl;
        std::cout << "[" << name << "] listening on " << addr
                  << " peers=" << peers_csv << std::endl;
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

        MultiPaxosReplica service(
            name, is_leader, peer_name_to_addr);  // create a server structure

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

        std::cout << "[" << name << "] listening on MultiPaxos replica " << addr
                  << " peers=" << map_to_string(peer_name_to_addr)
                  << " is_leader=" << std::boolalpha << is_leader << std::endl;
        server->Wait();
        return 0;
    } else {
        std::cerr << "Unknown application type. Use 'b' for broadcast or 'mp' "
                     "for multi-paxos.\n";
        return 1;
    }
}
