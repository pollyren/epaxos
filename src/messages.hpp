#ifndef EPAXOS_MESSAGES_HPP
#define EPAXOS_MESSAGES_HPP

#include "types.hpp"

namespace epaxos {

// message types
enum class MessageType {
    PRE_ACCEPT,
    PRE_ACCEPT_REPLY,
    ACCEPT,
    ACCEPT_REPLY,
    COMMIT
};

// message structure
struct Message {
    MessageType type;
    ReplicaId sender;
    ReplicaId receiver;

    Message(MessageType t, ReplicaId s, ReplicaId r)
        : type(t), sender(s), receiver(r) {}
    virtual ~Message() = default;
};

} // namespace epaxos

#endif // EPAXOS_MESSAGES_HPP
