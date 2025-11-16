#ifndef MP_TYPES_HPP
#define MP_TYPES_HPP

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace multipaxosTypes {

// basic identifiers
using ReplicaId = std::string;
using InstanceSeqId = int64_t;
using Key = std::string;
using Value = std::string;

struct Replica {
    ReplicaId id;
    std::string address;  // network address of the replica
    std::vector<std::vector<struct Instance>> instances;  // instance space

    Replica(ReplicaId rid, const std::string& addr)
        : id(rid), address(addr), instances() {}
};

// command structure
struct Command {
    enum Action {
        DUMMY,
        NOOP,           // no operation
        READ,           // read key-value pair
        WRITE,          // write key-value pair
        DELETE          // delete key
    } action;           // action type
    std::string key;    // key for the operation
    std::string value;  // value for write operation
};

// instance status
enum class Status {
    NONE,  // default initial value
    PREPARED,
    ACCEPTED,
    COMMITTED
};

struct InstanceID {
    ReplicaId replica_id;
    InstanceSeqId replicaInstance_id;

    InstanceID() : replica_id(), replicaInstance_id(0) {}
    InstanceID(ReplicaId rid, InstanceSeqId iid)
        : replica_id(rid), replicaInstance_id(iid) {}
};

// instance structure
struct Instance {
    Command cmd;
    Status status;
    InstanceID id;

    Instance() : status(Status::NONE) {}
};

}  // namespace multipaxosTypes

#endif  // MP_TYPES_HPP
