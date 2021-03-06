# Java SDK example: REST service, copy Raw tables
This is an example of how to build a REST service based on the CDF Java SDK. 

We use Quarkus (https://quarkus.io/) as the framework for exposing the REST endpoints. 
This gives us out of the box handling of packaging (as a container), endpoints, logging, configuration, 
metrics, openapi spec, and more. 

The example service uses the Java SDK to copy all rows from one CDF Raw table to another. 


## Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.

## Packaging and running the application

The application can be packaged using:
```shell script
./mvnw package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

If you want to build an _über-jar_, execute the following command:
```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

## Creating a native executable

You can create a native executable using: 
```shell script
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/code-with-quarkus-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/maven-tooling.html.

## Related guides

- Micrometer metrics ([guide](https://quarkus.io/guides/micrometer-metrics)): Instrument the runtime and your application with dimensional metrics using Micrometer.

## Provided examples

### RESTEasy Reactive example

Rest is easy peasy & reactive with this Hello World RESTEasy Reactive resource.

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)
