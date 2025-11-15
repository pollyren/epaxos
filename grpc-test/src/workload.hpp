#ifndef WORKLOAD_HPP
#define WORKLOAD_HPP

#include <string>
#include <vector>

namespace workload {

// types of operations
enum class OperationType {
    OP_READ,
    OP_WRITE,
    OP_BROADCAST,
    OP_GET_STATE,
};

// struct to represent a single operation
// operation can be read, write, broadcast or get_state
struct Operation {
    OperationType type;
    std::string key;
    std::string value;   // value to write, empty for reads
    std::string server;  // target server address

    Operation(OperationType t, const std::string& k, const std::string& v = "",
              const std::string& s = "")
        : type(t), key(k), value(v), server(s) {}
};

// class for parsing csv workload files
class CSVParser {
   public:
    // parse a csv file and return list of operations
    // expected csv format: operation,key,value,server
    static std::vector<Operation> parse(const std::string& filename);

   private:
    static OperationType parse_type(const std::string& op_str);
    static std::string trim(const std::string& str);
};

}  // namespace workload

#endif  // WORKLOAD_HPP
