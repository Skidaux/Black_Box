#include "minielastic/Analyzer.hpp"

#include <cctype>

namespace minielastic {

std::vector<std::string> Analyzer::tokenize(const std::string& text) {
    std::vector<std::string> tokens;
    std::string current;

    for (unsigned char ch : text) {
        if (std::isalnum(ch)) {
            current.push_back(std::tolower(ch));
        } else {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        }
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }

    return tokens;
}

} // namespace minielastic
