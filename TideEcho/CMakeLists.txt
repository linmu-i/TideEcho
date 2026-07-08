cmake_minimum_required (VERSION 3.12)

project(TideEcho VERSION 1.0.0)
add_library(TideEcho STATIC)

target_compile_features(TideEcho PRIVATE cxx_std_20)

target_include_directories(TideEcho PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
if (WIN32)
	target_link_libraries(TideEcho PUBLIC "iphlpapi.lib" "ws2_32.lib")
endif()

target_sources(TideEcho PRIVATE "src/TideEcho.cpp")