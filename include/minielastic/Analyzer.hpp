#pragma once

#include <string>
#include <vector>

namespace minielastic {

// Simple text analyzer for tokenization; split on non-alnum and lowercase.
class Analyzer {
public:
    static std::vector<std::string> tokenize(const std::string& text);
};

} // namespace minielastic
