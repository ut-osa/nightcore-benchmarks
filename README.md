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
EC2 VMs for running experiments use public AMI (`ami-06e206d7334bff2ec`) built by us,
which is based on Ubuntu 20.04 with necessary dependencies installed.

### Installation ###

Our scripts will use public Docker images (built by us as well) hosted on DockerHub to run experiments.
Thus no compilation is needed. But it requires a controller machine setting up to conduct Nightcore experiments.

#### Setting up the controller machine ####

Our scripts require a controller machine in AWS `us-east-2` region to provision and control experiment VMs.
The controller machine can use very small EC2 instance type, as it does not affect experimental results.
In our setup, we use a `t3.micro` EC2 instance installed with Ubuntu 20.04 as the controller machine.

The controller machine needs `bash`, `python3`, `rsync`, and AWS CLI version 1 installed.
`python3` and `rsync` can be installed with `apt`,
and this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-linux.html)
details the recommanded way for installing AWS CLI version 1.
Once installed, AWS CLI has to be configured with region `us-east-2` and access key
(see this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)).

Then on the controller machine, clone this repository with all submodules
```
git clone --recursive https://github.com/ut-osa/nightcore-benchmarks.git
```
Finally, executing `scripts/setup_sshkey.sh` to setup SSH keys that will be used to access experiment VMs.
Please read the notice in `scripts/setup_sshkey.sh` before executing it to see if this script works for your setup.

#### Setting up EC2 security group and placement group ####

Our VM provisioning script creates EC2 instances with security group `nightcore` and placement group `nightcore-experiments`.
The security group includes firewall rules for experiment VMs (including allowing the controller machine to SSH into them),
while the placement group instructs AWS to place experiment VMs close together.
Executing `scripts/aws_provision.sh` will create these groups with the correct configurations.

### Experiment workflow ###

Individual experiments correspond to directories inside `experiments`.
The script `experiments/*/run_all.sh` contains the end-to-end workflow to running experiments of the corresponding workload.

### Evaluation and expected result ###

For each experiment, the evaluation metric is the latency distribution under a specific QPS.
We use `wrk2` as the benchmarking tool, and it will output a detail latency distribution.
`experiments/*/expected_results` contain examples of expected outputs.
The file `experiments/*/expected_results/*/wrk2.log` contains the `wrk2` output, from where we report 50% and 99% latency in our paper.

### Notes ###

* `scripts/docker_images.sh` includes details about how we build Docker images for experiments.
But note that Docker in experiment VMs will always pull images from DockerHub.
Thus if you are going to customize these images, you need to use image repositories under your own DockerHub account.

### License ###

* [Nightcore](https://github.com/ut-osa/nightcore), Google's microservice demo,
`runc`, and `wrk2` are licensed under Apache License 2.0.
* DeathStarBench (`workloads/DeathStarBench`) is licensed under GNU General Public License v2.0.
* All other source code in this repository is licensed under Apache License 2.0.

