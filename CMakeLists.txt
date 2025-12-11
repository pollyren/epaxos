cmake_minimum_required(VERSION 3.16)
project(grpc_3srv_1cli LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Tip (Homebrew/macOS): pass -DCMAKE_PREFIX_PATH=/opt/homebrew when configuring
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(EPAXOS_PROTO_FILE ${PROTO_DIR}/epaxos.proto)
set(MP_PROTO_FILE ${PROTO_DIR}/multipaxos.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g -O1")
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")

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
    ${EPAXOS_PROTO_FILE}
  DEPENDS ${EPAXOS_PROTO_FILE}
  VERBATIM
)

add_custom_command(
  OUTPUT
    ${GEN_DIR}/multipaxos.pb.cc
    ${GEN_DIR}/multipaxos.pb.h
    ${GEN_DIR}/multipaxos.grpc.pb.cc
    ${GEN_DIR}/multipaxos.grpc.pb.h
  COMMAND protobuf::protoc
  ARGS
    --proto_path=${PROTO_DIR}
    --cpp_out=${GEN_DIR}
    --grpc_out=${GEN_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${MP_PROTO_FILE}
  DEPENDS ${MP_PROTO_FILE}
  VERBATIM
)

add_library(epaxos_proto
  ${GEN_DIR}/epaxos.pb.cc
  ${GEN_DIR}/epaxos.grpc.pb.cc
)
target_include_directories(epaxos_proto PUBLIC ${GEN_DIR})
target_link_libraries(epaxos_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(server
    src/server.cpp
    src/mp_server.cpp
    src/ep-server.cpp
    src/utils.cpp
)

add_executable(client
    src/client.cpp
    src/mp_client.cpp
    src/ep-client.cpp
    src/workload.cpp
    src/utils.cpp
)

add_library(multipaxos_proto
  ${GEN_DIR}/multipaxos.pb.cc
  ${GEN_DIR}/multipaxos.grpc.pb.cc
)
target_include_directories(multipaxos_proto PUBLIC ${GEN_DIR})
target_link_libraries(multipaxos_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

target_link_libraries(server PRIVATE multipaxos_proto epaxos_proto gRPC::grpc++)
target_link_libraries(client PRIVATE multipaxos_proto epaxos_proto gRPC::grpc++)

add_custom_target(copy_workload ALL
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_SOURCE_DIR}/workload.csv
        ${CMAKE_BINARY_DIR}/workload.csv
    DEPENDS ${CMAKE_SOURCE_DIR}/workload.csv
)

add_compile_options(-fsanitize=address -g -O1)
add_link_options(-fsanitize=address)