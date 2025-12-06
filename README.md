# CloudLab Experiment Instructions

<h2>Step 1: Prepare binaries for client and server</h2>
<p>SSH into the client-0-0 machine, then clone the repository code:</p>
<pre>
  $ git clone https://github.com/pollyren/epaxos.git
</pre>

<p>Install cmake:</p>
<pre>
  $ apt install cmake
  $ sudo apt update && sudo apt install -y build-essential autoconf libtool pkg-config cmake git
</pre>

<p>Install gRPC (this step might take 5-10 min):</p>
<pre>
  $ git clone --recurse-submodules -b v1.65.0 https://github.com/grpc/grpc
  $ cd grpc
  $ mkdir build && cd build
  $ cmake .. -DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF
  $ make -j$(nproc)
  $ sudo make install
  $ sudo ldconfig
</pre>

<p>Create build folder and compile C++ code:</p>
<pre>
  $ cd ../..
  $ mkdir build && cd build
  $ cmake .. -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF
  $ make -j$(nproc)
  $ cd ..
  $ cmake -S . -B build
  $ cmake --build build -j
</pre>

<p>You should now see a build folder in the top level directoty. Inside, you should also see the compiled client, server, and workload.csv files, along with the generated folder.</p>

<h2>Step 2: Run experiment</h2>
The configuration file for the experiment is located inside the experiments folder. You can modify the parameters to change the experiment configurations. To run the experiment, execute:
<pre>
  $ python3 scripts/run_multiple_experiments.py experiments/3-replicas-wan.json
</pre>

<p>The results of the run will appear under experiments/results. To delete the results folder before running a new run, execute:</p>
<pre>
  $ rm -rf experiments/results
</pre>
