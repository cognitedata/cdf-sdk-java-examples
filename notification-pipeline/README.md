## Notification pipeline

Transform for building a notification event from CDF.Raw table(s).

The source is SAP notifications extracted via the SAP OData api to CDF.Raw. This pipeline then reads from the 
raw table and performs a simple transform into CDF Events:
- One row in raw -> one event in CDF Clean.
- Map fields.
- Add asset links.


## Deployment
The extractor is preconfigured for deployment via Skaffold:
````powershell
skaffold run -n <namespace>
````