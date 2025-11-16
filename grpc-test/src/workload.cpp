#include "workload.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace workload {

std::vector<Operation> CSVParser::parse(const std::string& filename) {
    std::vector<Operation> operations;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("CSVParser::parse: unable to open file: " +
                                 filename);
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string op, key, value, server;

        // expected csv format: operation,key,value,server
        if (!std::getline(ss, op, ',')) continue;
        if (!std::getline(ss, key, ',')) continue;
        if (!std::getline(ss, value, ',')) value = "";
        if (!std::getline(ss, server, ',')) server = "";

        op = trim(op);
        key = trim(key);
        value = trim(value);
        server = trim(server);

        OperationType op_type = parse_type(op);
        operations.emplace_back(op_type, key, value, server);
    }

    file.close();
    return operations;
}

OperationType CSVParser::parse_type(const std::string& op_str) {
    if (op_str == "read") {
        return OperationType::OP_READ;
    } else if (op_str == "write") {
        return OperationType::OP_WRITE;
    } else if (op_str == "broadcast") {
        return OperationType::OP_BROADCAST;
    } else if (op_str == "get_state") {
        return OperationType::OP_GET_STATE;
    } else {
        throw std::runtime_error("CSVParser::parse_type: unknown operation: " +
                                 op_str);
    }
}

std::string CSVParser::trim(const std::string& str) {
    auto a = str.find_first_not_of(" \t\n\r");
    if (a == std::string::npos) {  // handle case where string is all whitespace
        return "";
    }

    auto b = str.find_last_not_of(" \t\n\r");

    std::string out = str.substr(a, b - a + 1);

    // also trim surrounding quotes if present
    if (out.size() >= 2 && out.front() == '"' && out.back() == '"') {
        out = out.substr(1, out.size() - 2);
    }

    return out;
}

}  // namespace workload