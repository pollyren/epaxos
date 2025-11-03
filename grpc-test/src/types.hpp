#ifndef EPAXOS_TYPES_HPP
#define EPAXOS_TYPES_HPP

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>

namespace epaxosTypes {

// basic identifiers
using ReplicaId = int;
using InstanceId = int;
using Key = std::string;
using Value = std::string;

struct Replica {
    ReplicaId id;
    std::string address; // network address of the replica

    // metadata for other replicas on fast and slow paths
    // ...

    std::vector<std::vector<struct Instance>> instances; // instance space
    std::map<Key, int> conflicts; // TODO: check, conflicts

    Replica(ReplicaId rid, const std::string& addr)
        : id(rid), address(addr), instances(), conflicts() {}
};

// command structure
struct Command {
    enum Action {
        NOOP,               // no operation
        READ,               // read key-value pair
        WRITE,              // write key-value pair
        DELETE              // delete key
    } action;               // action type
    std::string key;        // key for the operation
    std::string value;      // value for write operation
};

// ballot structure
struct Ballot {
    int number; // ballot number for ordering
    ReplicaId replica_id; // id of the replica that created the ballot

    Ballot() : number(0), replica_id(0) {}
    Ballot(int n, ReplicaId r) : number(n), replica_id(r) {}

    bool operator<(const Ballot& other) const {
        if (number != other.number) return number < other.number;
        return replica_id < other.replica_id;
    }

    bool operator==(const Ballot& other) const {
        return number == other.number && replica_id == other.replica_id;
    }

    bool operator>(const Ballot& other) const {
        return other < *this;
    }
};

// instance status
enum class Status {
    NONE, // default initial value
    PREPARED,
    PRE_ACCEPTED,
    ACCEPTED,
    COMMITTED,
    EXECUTED
};

// instance attributes
struct InstanceAttr {
    int seq;  // sequence number
    std::vector<Instance> deps;  // dependencies

    /*InstanceAttr() : seq(0) {}
    InstanceAttr(int s, const std::set<std::pair<ReplicaId, InstanceId>>& d)
        : seq(s), deps(d) {}*/
};

// instance structure
struct Instance {
    Command cmd;
    Ballot ballot;
    Status status;
    InstanceAttr attr;

    Instance() : status(Status::NONE) {}
};

} // namespace epaxos

#endif // EPAXOS_TYPES_HPP
