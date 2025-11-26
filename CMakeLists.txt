cmake_minimum_required(VERSION 3.20)
project(grpc_3srv_1cli LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Tip (Homebrew/macOS): pass -DCMAKE_PREFIX_PATH=/opt/homebrew when configuring
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/epaxos.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

# Generate *.pb.cc / *.grpc.pb.cc
add_custom_command(
  OUTPUT
    ${GEN_DIR}/epaxos.pb.cc
    ${GEN_DIR}/epaxos.pb.h
    ${GEN_DIR}/epaxos.grpc.pb.cc
    ${GEN_DIR}/epaxos.grpc.pb.h
  COMMAND protobuf::protoc
  ARGS
    --proto_path=${PROTO_DIR}
    --cpp_out=${GEN_DIR}
    --grpc_out=${GEN_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  VERBATIM
)

add_library(epaxos_proto
  ${GEN_DIR}/epaxos.pb.cc
  ${GEN_DIR}/epaxos.grpc.pb.cc
)
target_include_directories(epaxos_proto PUBLIC ${GEN_DIR})
target_link_libraries(epaxos_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(server src/server.cpp)
add_executable(client
  src/client.cpp
  src/workload.cpp
)
target_link_libraries(server PRIVATE epaxos_proto)
target_link_libraries(client PRIVATE epaxos_proto)