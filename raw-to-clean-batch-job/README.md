## Batch job example: Raw to clean transform

This example illustrates how to implement a batch job and package it as a container. All job parameters are
configured via environment variables. 

The job needs an external scheduler to control its execution. You can use K8's own scheduler, or an orchestration
tool like Airflow (https://airflow.apache.org) or Argo (https://argoproj.github.io/projects/argo).