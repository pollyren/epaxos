#include "utils.h"

#include <algorithm>
#include <map>
#include <sstream>
#include <string>

void trim(std::string& t) {
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

ZipfGenerator::ZipfGenerator(size_t n, double s)
    : n_(n), s_(s), dist_(0.0, 1.0), rng_(std::random_device{}())
{
    H_ = 0.0;
    for (size_t i = 1; i <= n_; i++)
        H_ += 1.0 / std::pow(i, s_);
}

size_t ZipfGenerator::next() {
    double u = dist_(rng_) * H_;
    double sum = 0.0;

    for (size_t i = 1; i <= n_; i++) {
        sum += 1.0 / std::pow(i, s_);
        if (sum >= u) return i;
    }

    return n_;
}