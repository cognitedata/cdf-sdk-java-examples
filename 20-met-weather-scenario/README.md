## Met weather scenario

The Met weather scenario integrates weather observations and forecasts with Cognite Data Fusion. It covers the following data categories:
- Met alerts

### Met alerts

Met alerts are weather forecast alerts issued by the Norwegian Meteorological Institute. 


The data pipeline performs the following tasks:
1) Read the main input from a `CDF.Raw` table.


```mermaid
flowchart LR
    A[Met Alerts RSS] -->|read| B(RSS Extractor)
    subgraph CDF.Raw
        C[(Met.rss)]
        D[(Met.cap)]
    end
    B -->|write| C
    C -->|read| E(Cap Extractor)
    F[Met Alerts Cap] -->|read| E
    E -->|write| D
    subgraph CDF.Clean
        G[Events]
        H[Assets]
    end
    D -->|read| I(Alerts pipeline)
    I -->|write| H
```

