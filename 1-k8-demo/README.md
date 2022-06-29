# K8s Demo

A "hello world" example of how to implement a containerized Java batch job (extractor, pipeline, 
algorithm) that runs on a schedule.

This module illustrates the following capabilities:
- [Metrics for monitoring](#metrics)
- Logging
- [Configuration via file and env. variables](#configuration)
- Wrapping everything nicely into a container

## Quickstart

You can run this module in several ways: 1) locally as a Java application, 2) locally as a container on K8s, 
3) on a remote K8s cluster. All options allow you to both run but also enjoy a full debugging developer experience.

### Run as a local Java application

The minimum requirements for running the module locally:
- Java 11 SDK
- Maven

On Linux/MaxOS:
```console
$ mvn compile exec:java -Dexec.mainClass="com.cognite.sa.Demo"
```

On Windows Powershell:
```ps
> mvn compile exec:java -D exec.mainClass="com.cognite.sa.Demo"
```

### Run as a container on Kubernetes

Minimum requirements for running the module on K8s:
- Java 11 SDK
- Maven
- Skaffold
- Local K8s with kubectl

Make sure your kube context points to the K8s cluster that you want to run the container on. For example, if you 
have Docker desktop installed, you should see something like the following:
```console
$ kubectl config current-context
docker-desktop
```

Then you can build and deploy the container using Skaffold's `dev` mode:
```console
$ skaffold dev
```
This will compile the code, build the container locally and deploy it as a `job` on your local K8s cluster. By using 
`skaffold dev` you also get automatic log tailing so the container logs will be output to your console. When the 
container job finishes, you can press `ctrl + c` and all resources will be cleaned up.

## Metrics

Metrics are exposed as `prometheus metrics` via Prometheus client libraries. You instrument your code by:
1. Import the libraries. Have a look at the `pom.xml` file.
2. Defining the metrics.
3. Populating/updating the metric values.
4. Exposing the metrics via a http endpoint (for services) or by pushing them to the gateway (for batch jobs).

`Demo.java` illustrates how to set up basic batch job metrics and push them to the metrics gateway.

## Configuration

Adding configurability allows you to easily reuse your data application by adjusting parameters at deployment time (as opposed to having to do code changes). `Smallrye Config` is a configuration library that enables configuration via yaml config files and environment variables. 

`Smallrye` will try to resolve a configuration entry by checking a set of "sources":
1. The default config file packaged with the code at `.src/main/resources/META-INF/microprofile.yaml`.
2. The optional, deployment-provided config file at `./config/config.yaml`.
3. Environment variables.
4. System properties (set via the cmd arguments).

A config source with a higher number will override a config source with a smaller number. I.e. an environment variable will override the config file setting.

`Demo.java` illustrates how to access the configuration settings in your code.

`./kubernetes-manifests/*` illustrate how to supply a configuration file when running this module as a container on K8s. The basic steps are as follows:
1) Define the `config.yaml` file with the settings you want to apply. This would typically be all the configuration settings except the secrets (i.e. keys, passwords, etc.).
2) Define the main application manifest, `k8-demo.job.yaml`. In this example, we use `Job` as the workload. You would typically use `CronJob`or `Deployment` in test/prod while `Job` is a good option for dev. In the manifest, we mount the configuration as a file for the container (your code) to read.
3) Bind it all together via `kustomization.yaml`. 
