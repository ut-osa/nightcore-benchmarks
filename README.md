Benchmark workloads of [Nightcore](https://github.com/ut-osa/nightcore)
==================================

This repository includes source code of evaluation workloads of Nightcore,
and scripts for running experiments.
It includes all materials for the artifact evaluation of our ASPLOS '21 paper.

### Structure of this repository ###

* `dockerfiles`: Dockerfiles for building relevant Docker containers.
* `workloads`: source code of modified [DeathStarBench](https://github.com/delimitrou/DeathStarBench)
and Google's [microservice demo](https://github.com/GoogleCloudPlatform/microservices-demo/tree/v0.1.5),
for running on Nightcore.
* `experiments`: setup scripts for running experiments of individual workloads.
* `scripts`: helper scripts for building Docker containers, and provisioning EC2 instances for experiments.
* `misc`: source code of modified `wrk2` (our benchmarking tool), and patch of our modification to Docker's `runc`.

### Hardware and software dependencies ###

Our evaluation workloads run on AWS EC2 instances in `us-east-2` region.

EC2 VMs for running experiments use a public AMI (`ami-06e206d7334bff2ec`) built by us,
which is based on Ubuntu 20.04 with necessary dependencies installed.

### Installation ###

#### Setting up the controller machine ####

A controller machine in AWS `us-east-2` region is required for running scripts executing experiment workflows.
The controller machine can use very small EC2 instance type, as it only provisions and controls experiment VMs,
but does not affect experimental results.
In our own setup, we use a `t3.micro` EC2 instance installed with Ubuntu 20.04 as the controller machine.

The controller machine needs `python3`, `rsync`, and AWS CLI version 1 installed.
`python3` and `rsync` can be installed with `apt`,
and this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-linux.html)
details the recommanded way for installing AWS CLI version 1.
Once installed, AWS CLI has to be configured with region `us-east-2` and access key
(see this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)).

Then on the controller machine, clone this repository with all git submodules
```
git clone --recursive https://github.com/ut-osa/nightcore-benchmarks.git
```
Finally, execute `scripts/setup_sshkey.sh` to setup SSH keys that will be used to access experiment VMs.
Please read the notice in `scripts/setup_sshkey.sh` before executing it to see if this script works for your setup.

#### Setting up EC2 security group and placement group ####

Our VM provisioning script creates EC2 instances with security group `nightcore` and placement group `nightcore-experiments`.
The security group includes firewall rules for experiment VMs (including allowing the controller machine to SSH into them),
while the placement group instructs AWS to place experiment VMs close together.

Executing `scripts/aws_provision.sh` on the controller machine creates these groups with correct configurations.

#### Building Docker images ####
We also provide the script (`scripts/docker_images.sh`) for building Docker images relevant to experiments in this artifact.
As we already pushed all compiled images to DockerHub, there is no need to run this script
as long as you do not modify source code of Nightcore (in `nightcore` directory) and evaluation workloads (in `workloads` directory).

### Experiment workflow ###

Each sub-directory within `experiments` corresponds to one experiment.
Within each experiment directory, a `config.json` file describes machine configuration and placement assignment of
individual Docker containers (i.e. microservices) for this experiment.

The entry point of each experiment is the `run_all.sh` script.
It first provisions VMs for experiments.
Then it executes evaluation workloads with different QPS targets via `run_once.sh` script.
`run_once.sh` script performs workload-specific setups, runs `wrk2` to measure latency distribution under the target QPS,
and stores results in `results` directory.
When everything is done, `run_all.sh` script terminates all provisioned experiment VMs.

VM provisioning is done by `scripts/exp_helper` with sub-command `start-machines`.
By default, it creates on-demand EC2 instances. But it also supports the option to
use Spot instances for cost saving.
After EC2 instances are up, the script then sets up Docker engines on newly created
VMs to form a Docker cluster in [swarm](https://docs.docker.com/engine/swarm/) mode.

### Evaluation and expected result ###

For each experiment, the evaluation metric is the latency distribution under a specific QPS.
We use `wrk2` as the benchmarking tool, and it outputs a detailed latency distribution, which looks like
```
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.21ms
 75.000%    3.29ms
 90.000%    5.13ms
 99.000%    9.12ms
 99.900%   12.28ms
 99.990%   17.45ms
 99.999%   20.32ms
100.000%   23.61ms
```
We report the 50\% and 99\% percentile values as median and tail latencies in the paper. 
`run_all.sh` script conducts evaluations on various QPS targets.

Experiment sub-directories ending with `singlenode` correspond to Nightcore results in Figure 7 the main paper.
Experiment sub-directories ending with `4node` correspond to Nightcore (4 servers) results in Table 4 of the main paper.
Note that `run_all.sh` scripts run less data points than presented in the paper, to allow a fast validation.
But all `run_all.sh` scripts can be easily modified to collect more data points.

We provide a helper script `scripts/collect_results` to print a summary of all experiment results.
Meanwhile, `expected_results_summary.txt` gives the summary generated from our experiment runs.
Details of our runs are stored in the `expected_results` directory within each experiment sub-directory.
Note that these results are not the exact ones presented in the paper.

### License ###

* [Nightcore](https://github.com/ut-osa/nightcore), Google's microservice demo,
`runc`, and `wrk2` are licensed under Apache License 2.0.
* DeathStarBench (`workloads/DeathStarBench`) is licensed under GNU General Public License v2.0.
* All other source code in this repository is licensed under Apache License 2.0.
