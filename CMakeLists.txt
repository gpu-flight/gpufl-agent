cmake_minimum_required(VERSION 3.30)
project(gpumon-agent LANGUAGES CUDA CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 17)

add_executable(gpumon-agent crawler/app_main.cpp crawler/MetricsSender.cpp crawler/GpuMonitor.cpp crawler/Config.cpp
        crawler/ClientLogReader.cpp crawler/JsonUtils.cpp
        crawler/app_main.cpp)

set_target_properties(gpumon-agent PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON)

target_link_libraries(gpumon-agent PRIVATE winhttp)