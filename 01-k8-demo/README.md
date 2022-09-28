# K8s Demo

A "hello world" example of how to implement a containerized Java batch job (extractor, pipeline, 
algorithm) that runs on a schedule.

This module illustrates the following capabilities:
- [Metrics for monitoring](#metrics)
- [Configuration via file and env. variables](#configuration)
- [Logging](#logging)
- [Wrapping everything nicely into a container](#Package-the-app-as-a-container)

## Quickstart

You can run this module in several ways: 1) locally as a Java application, 2) locally as a container on K8s, 3) on a remote K8s cluster. All options allow you to both run but also enjoy a full debugging developer experience.

### Run as a local Java application

The minimum requirements for running the module locally:
- Java 17 SDK
- Maven

On Linux/MaxOS:
```console
$ mvn compile exec:java -Dexec.mainClass="com.cognite.examples.Demo"
```

On Windows Powershell:
```ps
> mvn compile exec:java -D exec.mainClass="com.cognite.examples.Demo"
```

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
1. The default config file packaged with the code at [./src/main/resources/META-INF/microprofile.yaml](./src/main/resources/META-INF/microprofile.yaml).
2. The optional, deployment-provided config file at `./config/config.yaml`.
3. Environment variables.
4. System properties (set via the cmd arguments).

A config source with a higher number will override a config source with a smaller number. I.e. an environment variable will override the config file setting.

You should always provide a default config file packaged with the code at `./src/main/resources/META-INF/microprofile.yaml`. This file ensures you provide your module with sensible running defaults as well as serve as the configuration template for deployments. In the default config file you should also define the auth config keys, but not populate them with any values. Check the example [2-raw-to-clean-batch-job](../2-raw-to-clean-batch-job) for an illustration of how to deal with auth.

[Demo.java](./src/main/java/com/cognite/sa/Demo.java) illustrates how to access the configuration settings in your code. Have a look at the static members near the top of the file.

`./kubernetes-manifests/*` illustrate how to supply a configuration file when running this module as a container on K8s. The basic steps are as follows:
1) Define the `config.yaml` file with the settings you want to apply. This would typically be all the configuration settings except the secrets (i.e. keys, passwords, etc.).
2) Define the main application manifest, `k8-demo.job.yaml`. In this example, we use `Job` as the workload. You would typically use `CronJob`or `Deployment` in test/prod while `Job` is a good option for dev. In the manifest, we mount the configuration as a file for the container (your code) to read.
3) Bind it all together via `kustomization.yaml`. 

## Logging

We recommend following container logging best-practices and route log entries to `st out` and `st error`. This ensures that the logs will be picked up by K8s. Then your K8s infrastructure can forward the logs to the sink of your choice via a centralized infrastructure as opposed to configuring each individual code module.

In this example, we use [slf4j](https://www.slf4j.org/) and [Logback](https://logback.qos.ch/) as logging libraries. The logger configuration is defined at [./src/resources/logback.xml](./src/resources/logback.xml) and specifies logging to `st out/error`.

## Package the app as a container

There are several ways to package a Java application as a container: `Docker build`, [Jib](https://github.com/GoogleContainerTools/jib) and [Buildpacks](https://buildpacks.io/). Out of these options, `Jib` is a fast, mature and convenient option today while we expect `buildpacks` to become an improved alternative in the longer run.

This example illustrates how to use [Jib](https://github.com/GoogleContainerTools/jib) to build the container with [Skaffold](https://skaffold.dev/) as the orchestrator.

To enable `Jib`, just add it to the [Maven pom.xml](./pom.xml) as a build plugin:
```xml
<build>
    <plugins>
        <plugin>
            <groupId>com.google.cloud.tools</groupId>
            <artifactId>jib-maven-plugin</artifactId>
            <version>${jib.maven.plugin.version}</version>
            <configuration>
                <container>
                    <creationTime>USE_CURRENT_TIMESTAMP</creationTime>
                </container>
            </configuration>
        </plugin>
    </plugins>
</build>
```

Then we use the [skaffold.yaml](./skaffold.yaml) configuration to specify how to build the container. `Skaffold` has a native integration with `Jib` and will manage Jib's work. The `build` section in `skaffold.yaml` directs how the container is built:
```yaml
build:
  local:
    push: false                                            # When building locally, do not push the image to a repository
  artifacts:
    - image: k8-demo                                         # Image name
      context: .
      jib:                                                   # Use Jib as the container builder
        fromImage: "gcr.io/distroless/java17-debian11"       # Use a "distroless" base image
  tagPolicy:
    dateTime:                                              # The image will be tagged with the build timestamp
      format: "20060102T150405"
      timezone: "UTC"
```