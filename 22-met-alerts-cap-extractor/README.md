## Met Alerts CAP extractor

Met alerts are weather forecast alerts issued by the Norwegian Meteorological Institute. The alerts are published via an RSS feed which again links to a separate URI which carries the alert content (CAP). The Met alerts CAP extractor reads the CAP data payload from CAP URIs (produced by the RSS extractor).

 The CAP extractor adds a few capabililties on top of what the [RSS extractor](../21-met-alerts-rss-extractor/) design does. Most notably, it uses a `state store` to keep track of processed data item--this enables delta loading and recovery without having to perform full data reloads. In this specific case, the CAP source items are immutable and should only be read once. In addition, the extractor imploys an `upload queue` for writing data items to CDF.
 
 It uses the practices of logging, monitoring, configuration presented in [1-k8-demo](../1-k8-demo/README.md).

 The Met alerts api: [https://api.met.no/weatherapi/metalerts/1.1/documentation](https://api.met.no/weatherapi/metalerts/1.1/documentation)

The data pipeline performs the following tasks:
1) Read the CAP URIs from the `CDF Raw RSS table`.
2) Visit the URIs and retrieve the CAP payload.
2) Parse the CAP data to a `CDF Raw Row`.
3) Write the results to a `CDF.Raw` table.
4) Report status to `extraction pipelines`.

```mermaid
flowchart LR
    subgraph CDF.Raw
        1A[(Met.rss)]
        1B[(Met.cap)]
    end
    subgraph cap [CAP-extractor]
        direction LR
        2A(Read URIs)
        2B(Read CAP)
        2C(Parse CAP)
        2D(Write)
        2E(Report)
        2A --> 2B --> 2C --> 2D
        2D --> 2E
    end
    1A -->|read| 2A
    D{{API::Met Alerts Cap}} --->|read| 2B
    2D -->|write data| 1B
    subgraph es [CDF.Extraction-Pipelines]
        3A[Pipeline]
    end
    2E -->|Report Status| 3A
```

Design patterns to make note of:
- Using the `upload queue` to optimize writing to Raw: [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/utils.md](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/utils.md).
- Using `state store` to perform delta loads: [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/utils.md](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/utils.md).

## Quickstart

You can run this module in several ways: 1) locally as a Java application, 2) locally as a container on K8s, 3) on a remote K8s cluster. All options allow you to both run but also enjoy a full debugging developer experience.

### Run as a local Java application

The minimum requirements for running the module locally:
- Java 17 SDK
- Maven

On Linux/MaxOS:
```console
$ mvn compile exec:java -Dexec.mainClass="com.cognite.met.MetAlertsPipeline"
```

On Windows Powershell:
```ps
> mvn compile exec:java -D exec.mainClass="com.cognite.met.MetAlertsPipeline"
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