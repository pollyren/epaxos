rm -rf build
rm nohup.out
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/homebrew
cmake --build build -j

# three servers (each knows the other two)
nohup ./build/mp_server mp --name=S1 --port=50054 --peers=localhost:50052,localhost:50053  --peersName2Addr=S2===localhost:50052,S3===localhost:50053  --is_leader &
nohup ./build/mp_server mp --name=S2 --port=50052 --peers=localhost:50054,localhost:50053 &
nohup ./build/mp_server mp --name=S3 --port=50053 --peers=localhost:50054,localhost:50052 &

# client
sleep 3
nohup ./build/mp_client workload.csv

# close server
pkill -f ./build/mp_server