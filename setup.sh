# This is the script for setting up the EPaxos and Multi-Paxos experiments

# clone the EPaxos repository
git clone https://github.com/pollyren/epaxos.git
cd epaxos

# install the dependencies
sudo apt update
sudo apt install -y build-essential autoconf libtool pkg-config cmake git python3-numpy gnuplot

# clone the grpc repository
git clone --recurse-submodules -b v1.65.0 https://github.com/grpc/grpc

# build grpc
echo 'Starting grpc build...'
cd grpc
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF
make -j$(nproc)
sudo make install
sudo ldconfig
echo 'grpc build complete'

# build epaxos and multipaxos
echo 'Starting EPaxos and Multi-Paxos build...'
cd ../..
mkdir build && cd build
cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF
make -j$(nproc)
cd ..
cmake -S . -B build
cmake --build build -j
echo 'EPaxos and Multi-Paxos build complete'
