#ifndef EPAXOS_TYPES_HPP
#define EPAXOS_TYPES_HPP

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>

namespace epaxos {

// basic identifiers
using ReplicaId = int;
using InstanceId = int;

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
    PRE_ACCEPTED,
    ACCEPTED,
    COMMITTED,
    EXECUTED
};

// instance attributes
struct InstanceAttr {
    int seq;  // sequence number
    std::set<std::pair<ReplicaId, InstanceId>> deps;  // dependencies

    InstanceAttr() : seq(0) {}
    InstanceAttr(int s, const std::set<std::pair<ReplicaId, InstanceId>>& d)
        : seq(s), deps(d) {}
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
