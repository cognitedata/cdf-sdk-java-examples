## Batch job example: Interactive P&ID transform

This example illustrates how to implement a pipeline for building interactive P&IDs. It is implemented as a
batch job and package in a container. All job parameters can be configured via environment variables. 

The job needs an external scheduler to control its execution. You can use K8's own scheduler, or an orchestration
tool like Airflow (https://airflow.apache.org) or Argo (https://argoproj.github.io/projects/argo).

### Pipeline structure

Interactive P&IDs are built from "flat" P&ID documents (for example PDFs) which are scanned for text and symbols. The 
Cognite Data Fusion contextualization service scans the P&ID and creates annotations for identified text and symbols.
These annotations form the basis for linking the P&ID to `assets` and other `files`.

The main steps of the pipeline:
1. Read and process the CDF `assets` to use as basis for text matching.
2. Identify the candidate P&ID `files` to process.
3. Run the P&ID `files` through the contextualization service.
4. Upload the resulting SVG file to CDF.

