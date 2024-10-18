# Code for XDBC

This repository contains the code for XDBC.

## Build XDBC
To build XDBC you just need `make` and `docker`. Then, to build the XDBC Server and Client images run:
```
make
```
The images can also manually built `docker build ...` in the respective xdbc-server and xdbc-client directories.

## Set up the infrastructure
To spawn the XDBC Server and the supported systems run:
```
docker compose -f docker-xdbc.yml up -d
docker compose -f docker-tc.yml up -d
```
Docker tc is used for emulating network traffic restrictions on containers.

## Running XDBC

### First start the server
To run a data transfer simply run XDBC with the default options:
```
docker exec -it xdbcserver bash -c "./xdbc-server/build/xdbcserver"
```
The XDBC Server supports multiple options. For example, to transfer from a CSV source with a buffer size of 256 kb, a buffer pool size of 16384, and the parallelism for deserialization at 16, read at 1 and compression at 2 with a row format and snappy, run:
```
docker exec -it xdbcserver bash -c "./xdbc-server/build/xdbc-server \
--system csv -b 256 -p 16384 --deser-parallelism 16 --read-parallelism 1 \
--compression-parallelism=2 -f1 -csnappy"
Currently, XDBC assumes your data is placed in `/dev/shm`, which is also mapped to the containers' `/dev/shm`.
```
### Then initiate the transfer through a client
```
docker exec -it xdbcserver bash -c "./xdbc-server/tests/build/test --table lineitem_sf10"
```
The XDBC Client also supports multiple options. For example to transfer the lineitem dataset with a buffer size of, a buffer pool size of 16384, and the parallelism for writing at 16, decompression at 1, run:
```
docker exec -it xdbcclient bash -c "/xdbc-client/tests/build/test_xclient --table lineitem_sf10 \
-b 256 -p 16384 --write-parallelism 16 --decomp-parallelism=2"
```
Your output will be located at `/dev/shm/`
## Optimizer
The optimizer is currently implemented in Python, hence a working local Python installation is necessary.
To use XDBC's optimizer run:
```
python3 xdbc-client/optimizer/main.py --env_name=icu_analysis --optimizer=xdbc --optimizer_spec=heuristic
```
This will run the transfer in a predefined environment for benchmarking purposes. To add an environment change `xdbc-client/optimizer/test_envs.py`. If you run XDBC for the first time, it will first do a run to collect runtime statistics, to then run the optimizer for subsequent transfers.
