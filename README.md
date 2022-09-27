# Cognite Java SDK examples

This repository hosts a set of examples on how to use the Cognite Java SDK to interact with Cognite Data Fusion. The examples demonstrate various patterns, from general techniques like how to implement configurability, how to add monitoring to more specific patterns on how to build a streaming data pipeline and how to implement an interactive engineering diagram pipeline.

## Examples
- [01-k8-demo](./01-k8-demo/). Demonstrating how to set up configurability, monitoring, logging and building the container.
- [02-raw-to-clean-batch-job](./02-raw-to-clean-batch-job/). Illustrates a batch data pipeline parsing data from CDF.Raw to CDF.Event with contextualization.
- [20-met-weather-scenario](./20-met-weather-scenario/). A complete data product scenario including extractors and data pipelines for reading weather data produced by [Met Norway](https://api.met.no/).

## Quickstart

All examples are designed to run as container on your local computer. Some of them can also be run as standalone Java batch jobs. All options allow you to both run and enjoy a full debugging developer experience.

Navigate to the folder for your example, and start the application as a container or as a local application.

### Run as a container on Kubernetes

Minimum requirements for running the module on K8s:
- Java 17 SDK: [https://adoptium.net/](https://adoptium.net/)
- Maven: [https://maven.apache.org/download.cgi](https://maven.apache.org/download.cgi)
- Skaffold: [https://github.com/GoogleContainerTools/skaffold/releases](https://github.com/GoogleContainerTools/skaffold/releases)
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

### Run as a local Java application

The minimum requirements for running the module locally:
- Java 17 SDK
- Maven

On Linux/MaxOS:
```console
$ mvn compile exec:java -Dexec.mainClass="com.cognite.examples.TheClassToRun"
```

On Windows Powershell:
```ps
> mvn compile exec:java -D exec.mainClass="com.cognite.examples.TheClassToRun"
```
