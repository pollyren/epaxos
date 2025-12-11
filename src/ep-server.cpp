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
#include <mutex>

#include "../build/generated/epaxos.grpc.pb.h"
#include "absl/log/initialize.h"
#include "epaxos.pb.h"
#include "graph.hpp"
#include "types.hpp"
#include "utils.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace std::chrono;

#define ENABLE_EP_DEP_GRAPH_LOGGING 1
#if ENABLE_EP_DEP_GRAPH_LOGGING
static std::mutex COUT_LOCK;
#endif

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

class EPaxosReplica final : public demo::EPaxosReplica::Service {
   private:
    std::string thisReplica_;  // my name

    int instanceCounter_ = 0;  // instance counter

    // all peer addrs and stubs
    std::map<std::string, std::unique_ptr<demo::EPaxosReplica::Stub>>
        peersNameToStub_;

    int peerSize;
    // slow quorum addrs
    std::vector<std::string> fastQuorumNames_;

    // slow quorum addrs and stubs
    std::vector<std::string> slowQuorumNames_;

    // instance is a map from key(replica) to a vector of instances
    std::unordered_map<std::string, std::vector<struct epaxosTypes::Instance>>
        instances;

    // return one instance
    std::string vec_to_string(const std::vector<epaxosTypes::InstanceID>& v) {
        std::ostringstream oss;
        oss << '[';
        for (size_t i = 0; i < v.size(); ++i) {
            if (i) oss << ", ";
            oss << '(' << std::quoted(v[i].replica_id) << ", "
                << v[i].replicaInstance_id << ')';
        }
        oss << ']';
        return oss.str();
    }

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

    std::string printInstance(const epaxosTypes::Instance& inst) {
        std::ostringstream oss;
        oss << "Instance " << inst.id.replica_id << "."
            << inst.id.replicaInstance_id << " [cmd: action=" << inst.cmd.action
            << " key=" << inst.cmd.key << " value=" << inst.cmd.value
            << "; status=" << static_cast<int>(inst.status)
            << "; seq=" << inst.attr.seq
            << "; deps=" << vec_to_string(inst.attr.deps) << "]";
        return oss.str();
    }

    // build dependency graph

    // return a set of dependencies for a given command in the form of Q.i
    std::vector<epaxosTypes::InstanceID> findDependencies(
        const epaxosTypes::Command& cmd) {
        std::vector<epaxosTypes::InstanceID> deps;

        // scan through all instances to find dependencies
        for (const auto& [replica, instVec] : instances) {
            for (const auto& inst : instVec) {
                // if key over-laps and not a read command, add to deps
                if (inst.cmd.key == cmd.key &&
                    inst.cmd.action != epaxosTypes::Command::READ) {
                    deps.push_back(inst.id);
                }
            }
        }

        return deps;
    }

    int findMaxSeq(const std::vector<epaxosTypes::InstanceID>& deps) {
        if (deps.empty()) return 0;

        // get the instances corresponding to the dependency IDs
        const auto& depInstances = findInstancesByIds(deps);

        int maxSeq = -1;
        for (const auto& inst : depInstances) {
            if (inst.attr.seq > maxSeq) {
                maxSeq = inst.attr.seq;
            }
        }
        return maxSeq;
    }

    // return the instance given its ID Q.i
    epaxosTypes::Instance findInstanceById(const epaxosTypes::InstanceID id) {
        // check if instance exists
        if (instances.find(id.replica_id) == instances.end() ||
            id.replicaInstance_id >= instances[id.replica_id].size()) {
            throw std::runtime_error("FindInstanceById: Instance not found");
        } else {
            epaxosTypes::Instance inst =
                instances[id.replica_id][id.replicaInstance_id];

            // make sure the information of Q.i matches the instance found
            assert(inst.id.replica_id == id.replica_id &&
                   inst.id.replicaInstance_id == id.replicaInstance_id);

            return inst;
        }
    }

    // return a vector of instances given their IDs {P.i, Q.j, ...}
    std::vector<epaxosTypes::Instance> findInstancesByIds(
        const std::vector<epaxosTypes::InstanceID>& ids) {
        std::vector<epaxosTypes::Instance> insts;
        for (const auto& id : ids) {
            insts.push_back(findInstanceById(id));
        }
        return insts;
    }

    // execute the given instance and its dependencies in order
    std::string execute(const epaxosTypes::Instance newInstance, bool fastPath) {
        // mark the instance as committed
        epaxosTypes::Instance inst = newInstance;
        inst.status = epaxosTypes::Status::COMMITTED;
        instances[inst.id.replica_id][inst.id.replicaInstance_id] = inst;

        LOG("[" << thisReplica_
                  << "] Committed instance: " << printInstance(inst)
                  << std::endl);
        LOG("[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl);

        Graph<epaxosTypes::InstanceID, InstanceIDHash> depGraph =
            buildDependencyGraphForInstanceID(inst.id, inst.cmd.key, fastPath);

        // topological sort the dependency graph
        auto [isDAG, sortedIds] = depGraph.topologicalSort();

        LOG("[" << thisReplica_ << "] Execution order for instance "
                  << inst.id.replica_id << "." << inst.id.replicaInstance_id
                  << ": ");

        for (const auto& id : sortedIds) {
            LOG(id.replica_id << "." << id.replicaInstance_id << " ");
        }
        if (sortedIds.size() == 0) {
            LOG("(none) ");
        }
        if (isDAG) {
            LOG(" (DAG)");
        } else {
            LOG(" (not a DAG, cycle detected) DEBUG needed!");
        }

        // TODO: handle the case where there is a cycle in the dependency graph

        // assure the first in sorted order is the instance itself
        assert(sortedIds[0].replica_id == inst.id.replica_id &&
               sortedIds[0].replicaInstance_id == inst.id.replicaInstance_id);

        // test if all dependency (every other than the first) are committed
        // If not, spin wait (in real EPaxos, should fetch from other replicas)
        if (sortedIds.size() > 1) {
            for (size_t i = 1; i < sortedIds.size(); ++i) {
                epaxosTypes::Instance depInst = findInstanceById(sortedIds[i]);
                while (depInst.status != epaxosTypes::Status::COMMITTED) {
                    LOG("[" << thisReplica_
                              << "] Waiting for dependency instance: "
                              << printInstance(depInst) << " to be committed."
                              << std::endl);
                    // spin wait
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    depInst = findInstanceById(sortedIds[i]);
                }
            }
        }
        // spin wait over, now execute the commands in order

        std::string result;

        // execute in the reverse sorted order
        // this is for testing purpose only
        for (int i = sortedIds.size() - 1; i >= 0; --i) {
            epaxosTypes::Instance execInst = findInstanceById(sortedIds[i]);
            LOG("\n[" << thisReplica_
                      << "] Executing instance: " << printInstance(execInst)
                      << std::endl);
            // execute the command
            if (execInst.cmd.action == epaxosTypes::Command::WRITE) {
                LOG("[" << thisReplica_
                          << "] WRITE executed: key=" << execInst.cmd.key
                          << " value=" << execInst.cmd.value << std::endl);
                result = execInst.cmd.value;
            } else if (execInst.cmd.action == epaxosTypes::Command::READ) {
                LOG("[" << thisReplica_
                          << "] READ executed: key=" << execInst.cmd.key
                          << " value=<not implemented>" << std::endl);
            } else {
                LOG("[" << thisReplica_
                          << "] Unknown command action: " << execInst.cmd.action
                          << std::endl);
            }
        }

        return result;
    }

    Graph<epaxosTypes::InstanceID, InstanceIDHash>
    buildDependencyGraphForInstanceID(const epaxosTypes::InstanceID id, const std::string& key, bool fastPath) {
        // Create a graph to represent dependencies
        Graph<epaxosTypes::InstanceID, InstanceIDHash> depGraph(true);

        // Use a queue for BFS traversal
        std::queue<epaxosTypes::InstanceID> toVisit;
        std::unordered_set<std::string> visited;
        toVisit.push(id);

        // BFS to build the graph
        while (!toVisit.empty()) {
            epaxosTypes::InstanceID currentId = toVisit.front();
            toVisit.pop();

            std::string idStr = currentId.replica_id + "." +
                                std::to_string(currentId.replicaInstance_id);
            if (visited.find(idStr) != visited.end()) {
                continue;  // already visited
            }
            visited.insert(idStr);

            // Get the instance corresponding to currentId
            epaxosTypes::Instance currentInst = findInstanceById(currentId);
            depGraph.addVertex(currentId);

            // Add edges for dependencies
            for (const auto& depId : currentInst.attr.deps) {
                depGraph.addVertex(depId);
                depGraph.addEdge(currentId, depId);
                toVisit.push(depId);
            }
        }

        // print the detail of the dependency graph

        LOG("[" << thisReplica_ << "] Dependency graph for instance "
                  << id.replica_id << "." << id.replicaInstance_id << " built."
                  << std::endl);

        // Print the graph
        LOG("[" << thisReplica_
                  << "] Dependency Graph Edges:" << std::endl);
        int dependencyCount = 0;
        for (const auto& vertexIdStr : visited) {
            // parse vertexIdStr to InstanceID
            auto pos = vertexIdStr.find('.');
            std::string rid = vertexIdStr.substr(0, pos);
            int iid = std::stoi(vertexIdStr.substr(pos + 1));
            epaxosTypes::InstanceID vertexId{rid, iid};

            const auto& currentInst = findInstanceById(vertexId);
            for (const auto& depId : currentInst.attr.deps) {
                dependencyCount++;
                LOG("  " << vertexId.replica_id << "."
                          << vertexId.replicaInstance_id << " -> "
                          << depId.replica_id << "." << depId.replicaInstance_id
                          << std::endl);
            }
        }

        LOG("[" << thisReplica_ << "] Dependency (Edge) count: " << dependencyCount << std::endl);
        LOG("[" << thisReplica_ << "] Vertex count: " << depGraph.size() << std::endl);

        LOG("[" << thisReplica_ << "] Dependency list count: "
                  << findInstanceById(id).attr.deps.size() << std::endl);

        #if ENABLE_EP_DEP_GRAPH_LOGGING
        {
            std::lock_guard<std::mutex> guard(COUT_LOCK);
            auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
            std::cout << "write," << t.count() << "," << id.replicaInstance_id << ","
                    << dependencyCount << "," << depGraph.size() << "," << key << "," << fastPath << std::endl;
        }
        #endif

        return depGraph;
    }

    void commit(const epaxosTypes::Instance newInstance) {
        // mark the instance as committed
        epaxosTypes::Instance inst = newInstance;
        inst.status = epaxosTypes::Status::COMMITTED;
        instances[inst.id.replica_id][inst.id.replicaInstance_id] = inst;

        LOG("[" << thisReplica_
                  << "] Committed instance: " << printInstance(inst)
                  << std::endl);
        LOG("[" << thisReplica_ << "] Current replica state: \n"
                  << instances_to_string() << std::endl);
        // construct commit req message
        demo::CommitReq commitReq;
        demo::InstanceId* id = commitReq.mutable_id();
        id->set_replica_id(inst.id.replica_id);
        id->set_instance_seq_id(inst.id.replicaInstance_id);

        commitReq.set_seq(inst.attr.seq);

        for (const auto& dep : inst.attr.deps) {
            demo::InstanceId* depId = commitReq.add_deps();
            depId->set_replica_id(dep.replica_id);
            depId->set_instance_seq_id(dep.replicaInstance_id);
        }

        demo::Command* c = commitReq.mutable_cmd();
        c->set_action(static_cast<demo::Action>(inst.cmd.action));
        c->set_key(inst.cmd.key);
        c->set_value(inst.cmd.value);

        // send commit messages to all replicas asynchronously
        LOG("----------------------------\n[" << thisReplica_
                  << "] Sending Commit RPCs: " << std::endl);

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            demo::CommitReply reply;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<demo::CommitReply>>
                rpc;
        };

        for (const auto& [peerName, stub] : peersNameToStub_) {
            // allocate an async call object
            auto* call = new AsyncCall;

            // send the commit RPC
            call->rpc = stub->AsyncCommit(&call->ctx, commitReq, &cq);
            call->rpc->Finish(&call->reply, &call->status, call);

            LOG("  Sent Commit RPC to " << peerName << std::endl);
        }

        // we do not wait for commit replies because they are empty acks
        // and EPaxos proceeds without waiting for these commit acks
    }

    Status run_paxos_accept(epaxosTypes::Instance newInstance, demo::Command c,
                            demo::InstanceId id) {
        // prepare and send accept messages
        demo::AcceptReq acceptReq;

        // set instance to accepted
        instances[newInstance.id.replica_id][newInstance.id.replicaInstance_id]
            .status = epaxosTypes::Status::ACCEPTED;

        // prepare the seq
        acceptReq.set_seq(newInstance.attr.seq);

        // prepare the deps
        for (const auto& dep : newInstance.attr.deps) {
            demo::InstanceId* depId = acceptReq.add_deps();
            depId->set_replica_id(dep.replica_id);
            depId->set_instance_seq_id(dep.replicaInstance_id);
        }

        acceptReq.mutable_id()->CopyFrom(id);
        acceptReq.set_sender(thisReplica_);

        // send Accept to all slow quorum members
        LOG("----------------------------\n[" << thisReplica_
                  << "] Sending Accept RPCs: " << std::endl);

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            demo::AcceptReply reply;
            grpc::Status status;

            std::unique_ptr<grpc::ClientAsyncResponseReader<demo::AcceptReply>>
                rpc;
        };

        // create a mapping from peer name to its async call
        std::map<std::string, std::unique_ptr<AsyncCall>> calls;

        // now send async Accept RPCs to all slow quorum members
        for (const auto& peerName : slowQuorumNames_) {
            auto call = std::make_unique<AsyncCall>();

            call->rpc = peersNameToStub_[peerName]->AsyncAccept_(
                &call->ctx, acceptReq, &cq);

            // request notification when the operation finishes asynchronously
            call->rpc->Finish(&call->reply, &call->status,
                              (void*)peerName.data());

            // store the call in the map
            calls.emplace(peerName, std::move(call));

            LOG("  Sent Accept RPC to " << peerName << std::endl);
        }

        // collect all accept replies
        int remaining = slowQuorumNames_.size();
        std::map<std::string, demo::AcceptReply> acceptReplies;

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

        // collect accept replies
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

        commit(newInstance);

        // execute the instance, or at least, build the dependency graph
        std::string value;
        if (newInstance.cmd.action == epaxosTypes::Command::WRITE) {
            LOG("----------------------------\n[" << thisReplica_
                        << "] WRITE command detected for instance: "
                        << printInstance(newInstance)
                        << "; Skipping execution." << std::endl);
            value = "<successful>";
            buildDependencyGraphForInstanceID(newInstance.id, newInstance.cmd.key, false);
        } else {
            value = execute(newInstance, false);
            // we don't handle reads so the value is unused
        }

        return Status::OK;
    }

   public:
    EPaxosReplica(std::string name,
                  const std::map<std::string, std::string>& peer_name_to_addrs,
                  const std::vector<std::string>& fast_peer_names,
                  const std::vector<std::string>& slow_peer_names)
        : thisReplica_(std::move(name)) {
        peerSize = peer_name_to_addrs.size();
        // keep a list of peer addresses and stubs
        for (const auto& [name, addr] : peer_name_to_addrs) {
            peersNameToStub_[name] = demo::EPaxosReplica::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }

        // initialize a set of fast/slow quorum addresses
        for (const auto& a : fast_peer_names) {
            if (peer_name_to_addrs.find(a) == peer_name_to_addrs.end()) {
                throw std::runtime_error(
                    "Fast quorum name not found in peer list");
            }
            fastQuorumNames_.push_back(a);
        }
        for (const auto& a : slow_peer_names) {
            if (peer_name_to_addrs.find(a) == peer_name_to_addrs.end()) {
                throw std::runtime_error(
                    "Slow quorum name not found in peer list");
            }
            slowQuorumNames_.push_back(a);
        }

        // initialize instance map for each replica (peer)
        for (const auto& [name, addr] : peer_name_to_addrs) {
            instances[name] = std::vector<struct epaxosTypes::Instance>();
        }

        peerSize = peer_name_to_addrs.size();
    }

    Status ClientWriteReq(ServerContext* /*ctx*/, const demo::WriteReq* req,
                          demo::WriteResp* resp) override {
        LOG("----------------------------\n[" << thisReplica_
                  << "] Received ClientWriteReq: key=" << req->key()
                  << " value=" << req->value() << std::endl);

        epaxosTypes::Instance newInstance;
        newInstance.cmd.action = epaxosTypes::Command::WRITE;
        newInstance.cmd.key = req->key();
        newInstance.cmd.value = req->value();
        newInstance.status = epaxosTypes::Status::PRE_ACCEPTED;
        newInstance.id.replica_id = thisReplica_;
        newInstance.id.replicaInstance_id = instanceCounter_;
        instanceCounter_++;

        LOG("[" << thisReplica_
                  << "] Created new instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl);

        // add dependencies/Maxsequence
        auto deps = findDependencies(newInstance.cmd);
        newInstance.attr.deps = deps;
        newInstance.attr.seq = findMaxSeq(deps) + 1;

        instances[thisReplica_].push_back(newInstance);

        if (instances[thisReplica_].size() != instanceCounter_) {
            throw std::runtime_error("RPC failed: instance counter mismatch");
        }

        // Now prepare and send pre-accept messages
        demo::PreAcceptReq preAcceptReq;

        // prepare the command (gamma)
        demo::Command c;
        c.set_action(demo::Action::WRITE);
        c.set_key(req->key());
        c.set_value(req->value());
        preAcceptReq.mutable_cmd()->CopyFrom(c);

        // prepare the seq
        preAcceptReq.set_seq(newInstance.attr.seq);

        // prepare ids of the dependencies
        for (const auto& dep : deps) {
            demo::InstanceId id;
            id.set_replica_id(dep.replica_id);
            id.set_instance_seq_id(dep.replicaInstance_id);
            preAcceptReq.add_deps()->CopyFrom(id);
        }

        // prepare id L.i for this instance
        demo::InstanceId id;
        id.set_replica_id(newInstance.id.replica_id);
        id.set_instance_seq_id(newInstance.id.replicaInstance_id);
        preAcceptReq.mutable_id()->CopyFrom(id);
        preAcceptReq.set_sender(thisReplica_);

        //  send PreAccept to all fast quorum members
        LOG("----------------------------\n[" << thisReplica_
                  << "] Sending PreAccept RPCs: " << std::endl);

        auto preStart = high_resolution_clock::now();

        grpc::CompletionQueue cq;
        struct AsyncCall {
            grpc::ClientContext ctx;
            demo::PreAcceptReply reply;
            grpc::Status status;

            std::unique_ptr<
                grpc::ClientAsyncResponseReader<demo::PreAcceptReply>>
                rpc;
        };

        // create a mapping from peer name to its async call
        std::map<std::string, std::unique_ptr<AsyncCall>> calls;

        // now send async PreAccept RPCs to all fast quorum members
        for (const auto& peerName : fastQuorumNames_) {
            auto call = std::make_unique<AsyncCall>();

            call->rpc = peersNameToStub_[peerName]->AsyncPreAccept(
                &call->ctx, preAcceptReq, &cq);

            // request notification when the operation finishes asynchronously
            call->rpc->Finish(&call->reply, &call->status,
                              (void*)peerName.data());

            // store the call in the map
            calls.emplace(peerName, std::move(call));

            LOG("  Sent PreAccept RPC to " << peerName << std::endl);
        }

        // collect all preAccept replies
        int remaining = fastQuorumNames_.size();
        std::map<std::string, demo::PreAcceptReply> preAcceptReplies;

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

            preAcceptReplies[peerName] = call->reply;
            remaining--;
        }

        LOG("----------------------------\n[" << thisReplica_
                  << "] PreAccept Reply: " << std::endl);

        int agreeCount = 0;
        for (const auto& [name, reply] : preAcceptReplies) {
            LOG(" From: " << name << "  Reply Details: "
                      << " ok=" << (reply.ok() ? "true" : "false")
                      << " seq=" << reply.seq()
                      << " conflict=" << (reply.conflict() ? "true" : "false")
                      << std::endl);

            if (reply.ok() && !reply.conflict()) {
                agreeCount++;
            }
            if (agreeCount >= fastQuorumNames_.size()) {
                break;
            }
        }

        // deciding the fast path or slow path
        if (agreeCount >= fastQuorumNames_.size()) {
            auto preEnd = high_resolution_clock::now();
            // calculate request latency
            int64_t latency = duration_cast<nanoseconds>(preEnd - preStart).count();
            LOG("[" << thisReplica_
                      << "] PreAccept phase took " << latency
                      << "nanoseconds for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << std::endl);
            LOG("[" << thisReplica_
                      << "] PreAccept phase succeeded for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << "because agreeCount=" << agreeCount
                      << " >= " << "peerSize=" << peerSize
                      << "; Go to fast path" << std::endl);
            // commit the instance
            commit(newInstance);

            // execute the instance, or at least, build the dependency graph
            std::string value;
            if (newInstance.cmd.action == epaxosTypes::Command::WRITE) {
                LOG("----------------------------\n[" << thisReplica_
                          << "] WRITE command detected for instance: "
                          << printInstance(newInstance)
                          << "; Skipping execution." << std::endl);
                value = "<successful>";
                buildDependencyGraphForInstanceID(newInstance.id, newInstance.cmd.key, true);
            } else {
                value = execute(newInstance, true);
            }
            resp->set_status("write accepted");
            return Status::OK;
        } else {
            LOG("[" << thisReplica_
                      << "] PreAccept phase FAILED for instance: "
                      << newInstance.id.replica_id << "."
                      << newInstance.id.replicaInstance_id
                      << "; Go to slow path" << std::endl);

            return run_paxos_accept(newInstance, c, id);
        }

        return Status::OK;
    }

    Status PreAccept(ServerContext* /*ctx*/, const demo::PreAcceptReq* req,
                     demo::PreAcceptReply* resp) override {
        LOG("----------------------------\n"
                  << "[" << thisReplica_
                  << "] Received PreAcceptReq for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl);

        if (req->sender().empty()) {
            throw std::runtime_error("PreAcceptReq: replica_id is empty");
        }

        if (req->sender() != req->id().replica_id()) {
            throw std::runtime_error(
                "PreAcceptReq: sender and proposal mismatch");
        }

        // construct command from request
        epaxosTypes::Command cmd;
        cmd.action =
            static_cast<epaxosTypes::Command::Action>(req->cmd().action());
        cmd.key = req->cmd().key();
        cmd.value = req->cmd().value();

        LOG("  Command: action=" << req->cmd().action()
                  << " key=" << req->cmd().key()
                  << " value=" << req->cmd().value() << std::endl);

        // construct seq/dep from request
        auto seq = req->seq();
        auto deps = std::vector<epaxosTypes::InstanceID>();
        deps.reserve(1024);
        for (const auto& dep : req->deps()) {
            deps.emplace_back(dep.replica_id(), dep.instance_seq_id());
        }

        // construct instance ID from request
        epaxosTypes::InstanceID instanceId;
        instanceId.replica_id = req->id().replica_id();
        instanceId.replicaInstance_id = req->id().instance_seq_id();

        std::vector<epaxosTypes::InstanceID> proposedDeps =
            findDependencies(cmd);
        proposedDeps.reserve(deps.size() + proposedDeps.size());

        bool conflict;
        // determine max sequence number from dependencies
        int maxSeq = findMaxSeq(proposedDeps);

        // check if findDependencies(cmd) is a subset of deps to determine
        // conflict
        if (proposedDeps.size() > deps.size()) {
            conflict = true;
        } else {
            conflict = false;
            for (const auto& pd : proposedDeps) {
                bool found = false;
                for (const auto& d : deps) {
                    if (pd.replica_id == d.replica_id &&
                        pd.replicaInstance_id == d.replicaInstance_id) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    conflict = true;
                    break;
                }
            }
        }

        int proposedSeq;
        if (seq > maxSeq) {
            proposedSeq = seq;
            conflict = false;
        } else {
            proposedSeq = maxSeq + 1;
            conflict = true;
        }

        // union dependencies
        proposedDeps.insert(proposedDeps.end(), deps.begin(), deps.end());

        // remove duplicates from proposedDeps
        std::sort(proposedDeps.begin(), proposedDeps.end(),
                  [](const epaxosTypes::InstanceID& a,
                     const epaxosTypes::InstanceID& b) {
                      if (a.replica_id != b.replica_id)
                          return a.replica_id < b.replica_id;
                      return a.replicaInstance_id < b.replicaInstance_id;
                  });
        proposedDeps.erase(
            std::unique(proposedDeps.begin(), proposedDeps.end(),
                        [](const epaxosTypes::InstanceID& a,
                           const epaxosTypes::InstanceID& b) {
                            return a.replica_id == b.replica_id &&
                                   a.replicaInstance_id == b.replicaInstance_id;
                        }),
            proposedDeps.end());

        LOG("  Proposed Seq: " << proposedSeq
                  << "  Proposed Deps: " << vec_to_string(proposedDeps)
                  << ". Conflict: " << (conflict ? "true" : "false")
                  << std::endl);

        // store the instance locally
        epaxosTypes::Instance newInstance;
        newInstance.cmd = cmd;
        newInstance.status = epaxosTypes::Status::PRE_ACCEPTED;
        newInstance.id = instanceId;
        newInstance.attr.deps = proposedDeps;
        newInstance.attr.seq = proposedSeq;

        // resize instance vector if needed
        if (instances[instanceId.replica_id].size() <=
            instanceId.replicaInstance_id) {
            instances[instanceId.replica_id].resize(
                instanceId.replicaInstance_id + 1);
        }
        instances[instanceId.replica_id][instanceId.replicaInstance_id] =
            newInstance;

        LOG("[" << thisReplica_
                  << "] Stored new instance: " << newInstance.id.replica_id
                  << "." << newInstance.id.replicaInstance_id << std::endl);

        // prepare the reply message
        resp->set_ok(true);
        resp->set_sender(thisReplica_);
        resp->set_seq(proposedSeq);
        resp->set_already_committed(false);
        for (const auto& dep : proposedDeps) {
            demo::InstanceId* d = resp->add_deps();
            d->set_replica_id(dep.replica_id);
            d->set_instance_seq_id(dep.replicaInstance_id);
        }
        resp->set_conflict(conflict);
        // resp->set_status("PreAccept OK");

        return Status::OK;
    }

    Status Accept_(ServerContext* /* ctx */, const demo::AcceptReq* req,
                   demo::AcceptReply* resp) override {
        if (req->sender().empty()) {
            throw std::runtime_error("AcceptReq: replica_id is empty");
        }

        if (req->sender() != req->id().replica_id()) {
            throw std::runtime_error("AcceptReq: sender and proposal mismatch");
        }

        LOG("[" << thisReplica_ << "] Received Accept from "
                  << req->sender() << " for instance " << req->id().replica_id()
                  << "." << req->id().instance_seq_id() << " seq=" << req->seq()
                  << std::endl);

        // set instances[L][i] to accepted
        instances[req->id().replica_id()][req->id().instance_seq_id()].status =
            epaxosTypes::Status::ACCEPTED;

        // prepare the reply message, i.e., set ok to true
        resp->set_ok(true);
        resp->set_sender(thisReplica_);

        return Status::OK;
    }

    Status Commit(ServerContext* /*ctx*/, const demo::CommitReq* req,
                  demo::CommitReply* /*resp*/) override {
        LOG("[" << thisReplica_ << "] Received Commit for instance "
                  << req->id().replica_id() << "."
                  << req->id().instance_seq_id() << std::endl);

        // store the instance locally
        epaxosTypes::Instance newInstance;
        newInstance.status = epaxosTypes::Status::COMMITTED;
        // construct instance ID from request
        epaxosTypes::InstanceID instanceId;
        instanceId.replica_id = req->id().replica_id();
        instanceId.replicaInstance_id = req->id().instance_seq_id();
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
                  << "] Committed instance: " << instanceId.replica_id
                  << "." << instanceId.replicaInstance_id << std::endl);
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
    EchoServiceImpl(std::string name,
                    const std::vector<std::string>& peer_addrs)
        : name_(std::move(name)) {
        // keep a list of peer addresses and stubs
        for (const auto& a : peer_addrs) {
            peer_addrs_.push_back(a);
            peer_stubs_.push_back(demo::Echo::NewStub(
                grpc::CreateChannel(a, grpc::InsecureChannelCredentials())));
        }
    }

    Status Ping(ServerContext* /*ctx*/, const demo::PingReq* req,
                demo::PingResp* resp) override {
        resp->set_reply(std::string("pong: ") + req->msg());
        resp->set_from(name_);

        if (req->fanout()) {
            demo::BcastReq b;
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

    Status Broadcast(ServerContext* /*ctx*/, const demo::BcastReq* req,
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
            demo::BcastReq fwd = *req;  // copy
            fwd.set_ttl(req->ttl() - 1);
            std::thread([this, fwd]() {
                (void)broadcast_to_peers(fwd);
            }).detach();
        }
        return Status::OK;
    }

   private:  // internal state of server
    std::string name_;
    std::vector<std::string> peer_addrs_;
    std::vector<std::unique_ptr<demo::Echo::Stub>> peer_stubs_;
    std::mutex mu_;
    std::unordered_set<std::string> seen_;  // broadcast de-dup

    std::string make_uuid() const {
        static std::atomic<uint64_t> ctr{0};
        std::stringstream ss;
        ss << name_ << "-" << now_ns_str() << "-" << ++ctr;
        return ss.str();
    }

    std::vector<std::string> broadcast_to_peers(const demo::BcastReq& req) {
        std::vector<std::string> acks;
        for (size_t i = 0; i < peer_stubs_.size(); ++i) {
            demo::BcastResp r;
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
int run_ep_server(int argc, char** argv) {
    // Usage: ./server --name=S1 --port=50051
    // --peers=localhost:50052,localhost:50053
    absl::InitializeLog();

    std::string name;
    std::string port = "50051";
    std::string peers_csv;
    std::map<std::string, std::string> peer_name_to_addr;
    std::string application = argv[1];  // broadcast or epaxos

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
        LOG("Server listening on " << addr << " peers=" << peers_csv
                  << std::endl);
        LOG("[" << name << "] listening on " << addr
                  << " peers=" << peers_csv << std::endl);
        server->Wait();
        return 0;
    } else if (application == "e") {
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
            }
        }

        const auto peer_addrs = split(peers_csv, ',');
        std::vector<std::string> peer_names;
        for (const auto& addr : peer_addrs) {
            for (const auto& [name, map_addr] : peer_name_to_addr) {
                if (map_addr == addr) {
                    peer_names.push_back(name);
                }
            }
        }

        int f = peer_names.size() / 2;
        size_t fast_path_quorum_size = f + (f + 1) / 2;
        size_t slow_path_quorum_size = f + 1;

        LOG("[" << name << "] Determined f=" << f
                  << ", fast_path_quorum_size=" << fast_path_quorum_size
                  << ", slow_path_quorum_size=" << slow_path_quorum_size
                  << std::endl);

        std::vector<std::string> fast_path_quorum(peer_names.begin(), peer_names.begin() + fast_path_quorum_size - 1);
        std::vector<std::string> slow_path_quorum(peer_names.begin(), peer_names.begin() + slow_path_quorum_size - 1);


        EPaxosReplica service(name, peer_name_to_addr, fast_path_quorum,
                              slow_path_quorum);  // create a server structure

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

        LOG("[" << name << "] listening on EPaxos replica " << addr
                  << " peers=" << map_to_string(peer_name_to_addr) << std::endl);
        server->Wait();
        return 0;
    } else {
        std::cerr << "Unknown application type. Use 'b' for broadcast or 'e' "
                     "for epaxos.\n";
        return 1;
    }
}
