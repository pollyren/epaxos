rm-rf build
rm nohup.out
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/homebrew
cmake --build build -j


# three servers (each knows the other two)
nohup ./build/server e --name=S1 --port=50051 --peers=localhost:50052,localhost:50053 &
nohup ./build/server e --name=S2 --port=50052 --peers=localhost:50051,localhost:50053 &
nohup ./build/server e --name=S3 --port=50053 --peers=localhost:50051,localhost:50052 &

# client (with broadcast)
#nohup ./build/client b --fanout localhost:50051 "hello with fanout"
sleep 1
nohup ./build/client w "k121" "apple" localhost:50051 
#nohup ./build/client g localhost:50051  


# sample output: