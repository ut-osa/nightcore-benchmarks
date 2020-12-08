Benchmark workloads of [Nightcore](https://github.com/ut-osa/nightcore) for ASPLOS '21
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

This artifact runs on AWS EC2 instances in `us-east-2` region.
EC2 VMs for running experiments use public AMI (`ami-06e206d7334bff2ec`) provided by us,
which is based on Ubuntu Focal with necessary dependencies installed.

### Installation ###

#### Setting up the controller machine ####

This artifact requires a controller machine in `us-east-2` region,
to conduct Nightcore experiments.
The controller machine can use very small EC2 instance type,
as it only coordinates experiment VMs and does not affect experimental results.
In our setup, we use a `t3.micro` EC2 instance installed with Ubuntu 20.04 as the
controller machine.

The controller machine needs `bash`, `python3`, `rsync`, and AWS CLI version 1 installed.
`python3` and `rsync` can be installed with `apt`, and this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-linux.html)
details the recommanded way for installing AWS CLI version 1.
Once installed, AWS CLI has to be configured with region `us-east-2` and access key
(see this [documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)).

#### Setting up EC2 security group and placement group ####

### Experiment workflow ###

### Evaluation and expected result ###

### Notes ###

### License ###

* [Nightcore](https://github.com/ut-osa/nightcore), Google's microservice demo,
`runc`, and `wrk2` are licensed under Apache License 2.0.
* DeathStarBench (`workloads/DeathStarBench`) is licensed under GNU General Public License v2.0.
* All other source code in this repository is licensed under Apache License 2.0.

