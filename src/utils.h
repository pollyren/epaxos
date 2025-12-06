#pragma once
#include <map>
#include <string>

void trim(std::string& t);
std::map<std::string, std::string> parse_map_mixed_simple(
    const std::string& str);

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