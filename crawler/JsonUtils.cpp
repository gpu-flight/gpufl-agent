#include "JsonUtils.h"
#include <algorithm>
#include <cctype>
#include <string>

namespace gpumon {
    std::string JsonUtils::extractString(const std::string& json, const std::string& key) {
        const std::string needle = "\"" + key + "\":\"";
        size_t start = json.find(needle);
        if (start == std::string::npos) return "";
        start += needle.length();

        std::string result;
        result.reserve(32);

        for (size_t i = start; i < json.length(); ++i) {
            if (json[i] == '\\' && i + 1 < json.length()) {
                result += json[i + 1];
                i++;
            } else if (json[i] == '"') {
                break;
            } else {
                result += json[i];
            }
        }
        return result;
    }

    int64_t JsonUtils::extractInt(const std::string& json, const std::string& key) {
        const std::string needle = "\"" + key + "\":";
        size_t start = json.find(needle);
        if (start == std::string::npos) return 0;
        start += needle.length();

        size_t end = start;
        while (end < json.length()) {
            char c = json[end];
            if (!isdigit(c) && c != '-') break;
            end++;
        }

        if (start == end) return 0;

        try {
            return std::stoll(json.substr(start, end - start));
        } catch (...) {
            return 0;
        }
    }

    int64_t JsonUtils::extractNestedInt(const std::string& json, const std::string& arrayKey, const std::string& targetField) {
        const size_t arrayPos = json.find("\"" + arrayKey + "\":");
        if (arrayPos == std::string::npos) return 0;

        const size_t fieldPos = json.find("\"" + targetField + "\":", arrayPos);
        if (fieldPos == std::string::npos) return 0;

        size_t start = fieldPos + targetField.length() + 3;

        while (start < json.length() && isspace(json[start])) start++;

        size_t end = start;
        while (end < json.length()) {
            char c = json[end];
            if (!isdigit(c) && c != '-') break;
            end++;
        }

        if (start < end) {
            try {
                return std::stoll(json.substr(start, end - start));
            } catch (...) { return 0; }
        }
        return 0;
    }

}