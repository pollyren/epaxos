#include "utils.h"
#include <string>
#include <map>
#include <sstream>
#include <algorithm>

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