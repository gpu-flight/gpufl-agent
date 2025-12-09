#ifndef GPU_MONITORING_JSONUTILS_H
#define GPU_MONITORING_JSONUTILS_H
#include <string>
#include <cstdint>

namespace gpumon {
    class JsonUtils {
    public:
        // Extract string value: "key":"value"
        static std::string extractString(const std::string& json, const std::string& key);

        // Extract int value: "key":123
        static int64_t extractInt(const std::string& json, const std::string& key);

        // Extract a specific field from a nested JSON object array
        // Pattern: "arrayKey":[ { ..."targetField": 123 ... } ]
        static int64_t extractNestedInt(const std::string& json, const std::string& arrayKey, const std::string& targetField);
    };

}

#endif //GPU_MONITORING_JSONUTILS_H