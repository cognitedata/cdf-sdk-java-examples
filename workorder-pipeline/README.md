![java-main-ci](https://github.com/cognitedata/internal-sap-writeback/actions/workflows/java-main-ci-workorder-pipeline.yml/badge.svg)
## Workorder pipeline

Transform for building work order events from CDF.Raw table(s).

The source is SAP workorders extracted via the SAP OData api to CDF.Raw. This pipeline then reads from the 
raw table and performs a simple transform into CDF Events:
- One row in raw -> multiple events in CDF Clean:
  - One work order event.
  - 0..N work order operations events.
  - 0..N work order objects/items events.
  - 0..N work oder confirmation events.
- Map event fields.
- Add asset links.


## Deployment
The extractor is preconfigured for deployment via Skaffold:
````powershell
skaffold run -n <namespace>
````
