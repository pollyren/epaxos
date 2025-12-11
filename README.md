# EPaxos and Multi-Paxos in C++

This repository contains an implementation of EPaxos and Multi-Paxos in C++.

## Instructions for running the experiments on CloudLab

### Step 1: Prepare binaries for client and server
SSH onto the client-0-0 machine, clone this repository and run the setup script:
```
  $ git clone https://github.com/pollyren/epaxos.git
  $ cd epaxos
  $ sh setup.sh
```

This should create a `build/` directory, which should include the client and server binaries.

### Step 2: Run experiments
The configuration files for the experiment should be placed inside the `experiments/` folder. Make sure that the `base_local_exp_directory`, `base_remote_bin_directory_nfs`, `experiment_name`, `project_name` and `src_directory` parameters match your experimental configuration. You can also modify other parameters to change the experiment configurations. Then, to run the experiment with, for instance, the `3-replicas-wan.json` configuration, execute:
```
  $ sudo python3 scripts/run_multiple_experiments.py experiments/3-replicas-wan.json
```
Note: The command must be run with `sudo` to enable communication between different machines by way of public keys.