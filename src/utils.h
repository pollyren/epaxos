#pragma once
#include <map>
#include <string>
#include <random>
#include <cmath>

// logging macros
#define EP_MP_LOGGING 1
#define LOG(msg) do { if (EP_MP_LOGGING) std::cerr << msg; } while (0)

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

class ZipfGenerator {
public:
    ZipfGenerator(size_t n, double s);

    size_t next();

private:
    size_t n_;
    double s_;
    double H_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> dist_;
};