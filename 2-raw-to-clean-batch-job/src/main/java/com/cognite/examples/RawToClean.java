package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.RawRow;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RawToClean {
    private static Logger LOG = LoggerFactory.getLogger(RawToClean.class);

    // cdf auth config
    private static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("cdf.authentication.apiKey", String.class);
    private static final Optional<String> apiKeyGcp =
            ConfigProvider.getConfig().getOptionalValue("cdf.authentication.apiKeyGcp", String.class);

    // raw source tables
    private static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String rawTable =
            ConfigProvider.getConfig().getValue("source.table", String.class);

    // Metrics configs. From config file / env variables
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName = ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);


    /*
    Metrics section. Define the metrics to expose.
     */
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start time stamp").register(collectorRegistry);
    static final Gauge errorGauge = Gauge.build()
            .name("errors").help("Total job errors").register(collectorRegistry);

    public static void main(String[] args) throws Exception {
        Instant startInstant = Instant.now();
        int countRows = 0;

        // Get the source table via env variables
        String dbName = System.getenv("RAW_DB").split("\\.")[0];
        String dbTable = System.getenv("RAW_DB").split("\\.")[1];

        CogniteClient client = getClient();

        // Get the data set id
        String dataSetExternalId = System.getenv("DATASET_EXT_ID");
        if (null == dataSetExternalId) {
            // The data set external id is not set.
            String message = "DATASET_EXT_ID is not configured.";
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.info("Looking up the data set external id: {}.",
                dataSetExternalId);
        List<DataSet> dataSets = client.datasets()
                .retrieve(ImmutableList.of(Item.newBuilder().setExternalId(dataSetExternalId).build()));

        if (dataSets.size() != 1) {
            // The provided data set external id cannot be found.
            String message = String.format("The configured data set external id does not exist: %s", dataSetExternalId);
            LOG.error(message);
            throw new Exception(message);
        }

        // Set up the reader for the raw table
        LOG.info("Starting to read the raw table {}.{}.",
                dbName,
                dbTable);
        Iterator<List<RawRow>> iterator = client.raw().rows().list(dbName, dbTable);

        // Iterate through all rows in batches and write to clean. This will effectively "stream" through
        // the data so that we have constant memory usage no matter how large the data set is.
        while (iterator.hasNext()) {
            List<Event> events = iterator.next().stream()
                    .map(row -> {
                        // Collect all columns into the metadata bucket of the event
                        Map<String, String> metadata = row.getColumns().getFieldsMap().entrySet().stream()
                                .collect(Collectors.toMap((Map.Entry<String, Value> entry) -> entry.getKey(),
                                        entry -> entry.getValue().getStringValue()));

                        // Add basic lineage info
                        metadata.put("dataSource",
                                String.format("%s.%s.%s", row.getDbName(), row.getTableName(), row.getKey()));

                        // Build the event object
                        return Event.newBuilder()
                                .setExternalId(StringValue.of(row.getTableName() + row.getKey()))
                                .setDescription(StringValue.of(
                                        row.getColumns().getFieldsOrThrow("my-mandatory-field").getStringValue()))
                                .putAllMetadata(metadata)
                                .setDataSetId(dataSets.get(0).getId())
                                .build();
                    })
                    .collect(Collectors.toList());

            client.events().upsert(events);
            countRows += events.size();
        }

        LOG.info("Finished processing {} rows from raw. Duration {}",
                countRows,
                Duration.between(startInstant, Instant.now()));
    }

    /*
    Instantiate the cognite client based on an api key hosted in GCP Secret Manager (key vault).
     */
    private static CogniteClient getClient() throws Exception {
        // Instantiate the client
        LOG.info("Start instantiate the Cognite Client.");

        LOG.info("API key is hosted in Secret Manager.");
        String projectId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[0];
        String secretId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[1];
        return CogniteClient.ofKey(getGcpSecret(projectId, secretId, "latest"))
                .withBaseUrl(baseURL);
    }

    /*
    Push the current metrics to the push gateway.
     */
    private static void pushMetrics() {
        if (pushGatewayUrl.isPresent()) {
            try {
                LOG.info("Pushing metrics to {}", pushGatewayUrl);
                PushGateway pg = new PushGateway(new URL(pushGatewayUrl.get())); //9091
                pg.pushAdd(collectorRegistry, metricsJobName);
            } catch (Exception e) {
                LOG.warn("Error when trying to push metrics: {}", e.toString());
            }
        } else {
            LOG.warn("No metrics push gateway configured. Cannot push the metrics.");
        }
    }
}
