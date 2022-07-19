package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.RawRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RawToClean {
    private static Logger LOG = LoggerFactory.getLogger(RawToClean.class);

    /*
    CDF project config. From config file / env variables.
     */
    private static final String cdfHost =
            ConfigProvider.getConfig().getValue("cognite.host", String.class);
    private static final Optional<String> cdfProject =
            ConfigProvider.getConfig().getOptionalValue("cognite.project", String.class);
    private static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("cognite.apiKey", String.class);
    private static final Optional<String> clientId =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientId", String.class);
    private static final Optional<String> clientSecret =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientSecret", String.class);
    private static final Optional<String> aadTenantId =
            ConfigProvider.getConfig().getOptionalValue("cognite.azureADTenantId", String.class);
    private static final String[] authScopes =
            ConfigProvider.getConfig().getValue("cognite.scopes", String[].class);

    /*
    CDF.Raw source table configuration. From config file / env variables.
     */
    private static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String rawTable =
            ConfigProvider.getConfig().getValue("source.table", String.class);

    /*
    CDF data target configuration. From config file / env variables.
     */
    private static final Optional<String> targetDataSetExtId =
            ConfigProvider.getConfig().getOptionalValue("target.dataSetExternalId", String.class);
    private static final Optional<String> extractionPipelineExtId =
            ConfigProvider.getConfig().getOptionalValue("target.extractionPipelineExternalId", String.class);

    /*
    Metrics target configuration. From config file / env variables.
     */
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName =
            ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);

    /*
    Metrics section. Define the metrics to expose.
     */
    private static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    private static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    private static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start timestamp").register(collectorRegistry);
    private static final Gauge errorGauge = Gauge.build()
            .name("job_errors").help("Total job errors").register(collectorRegistry);
    private static final Gauge noElementsGauge = Gauge.build()
            .name("job_no_elements_processed").help("Number of processed elements").register(collectorRegistry);

    private CogniteClient cogniteClient = null;
    private OptionalLong dataSetIntId = null;

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        boolean executionError = false;
        try {
            // Execute the main logic
            new RawToClean().run();

            // The job completion metric is only added to the registry after job success,
            // so that a previous success in the Pushgateway isn't overwritten on failure.
            Gauge jobCompletionTimeStamp = Gauge.build()
                    .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
            jobCompletionTimeStamp.setToCurrentTime();
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
            executionError = true;
        } finally {
            if (enableMetrics) {
                pushMetrics();
            }
            if (executionError) {
                System.exit(1); // container exit code for execution errors, etc.
            }
        }
    }

    /*
    The main logic to execute.
     */
    private void run() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Starting raw to clean pipeline...");

        // Prepare the job start metrics
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();
        jobStartTimeStamp.setToCurrentTime();

        // Set up the reader for the raw table
        LOG.info("Starting to read the raw table {}.{}.",
                rawDb,
                rawTable);
        Iterator<List<RawRow>> rawIterator = getCogniteClient().raw().rows().list(rawDb, rawTable);

        // Iterate through all rows in batches and write to clean. This will effectively "stream" through
        // the data so that we have constant memory usage no matter how large the data set is.
        while (rawIterator.hasNext()) {
            List<Event> events = new ArrayList<>();
            for (RawRow row : rawIterator.next()) {
                Event event = parseRawRowToEvent(row);
                if (getDataSetIntId().isPresent()) {
                    event = event.toBuilder()
                            .setDataSetId(dataSetIntId.getAsLong())
                            .build();
                }

                events.add(event);
            }

            getCogniteClient().events().upsert(events);
            noElementsGauge.inc(events.size());
        }

        LOG.info("Finished processing {} rows from raw. Duration {}",
                noElementsGauge.get(),
                Duration.between(startInstant, Instant.now()));
        jobDurationTimer.setDuration();
    }

    private Event parseRawRowToEvent(RawRow row) {
        final String loggingPrefix = "parseRawRowToEvent() - ";

        // Collect all columns into the metadata bucket of the event
        Map<String, String> metadata = row.getColumns().getFieldsMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(),
                        entry -> entry.getValue().getStringValue()));

        // Add basic lineage info
        metadata.put("dataSource",
                String.format("CDF Raw: %s.%s.%s", row.getDbName(), row.getTableName(), row.getKey()));

        // Build the event object
        return Event.newBuilder()
                .setExternalId(row.getTableName() + row.getKey())
                .setDescription(row.getColumns().getFieldsOrThrow("my-mandatory-field").getStringValue())
                .putAllMetadata(metadata)
                .build();
    }

    /*
    Return the data set internal id.

    If the data set external id has been configured, this method will translate this to the corresponding
    internal id.
     */
    private OptionalLong getDataSetIntId() throws Exception {
        if (null == dataSetIntId) {
            if (dataSetIntId.isPresent()) {
                // Get the data set id
                LOG.info("Looking up the data set external id: {}.",
                        targetDataSetExtId.get());
                List<DataSet> dataSets = getCogniteClient().datasets()
                        .retrieve(ImmutableList.of(Item.newBuilder().setExternalId(targetDataSetExtId.get()).build()));

                if (dataSets.size() != 1) {
                    // The provided data set external id cannot be found.
                    String message = String.format("The configured data set external id does not exist: %s", targetDataSetExtId.get());
                    LOG.error(message);
                    throw new Exception(message);
                }
                dataSetIntId = OptionalLong.of(dataSets.get(0).getId());
            } else {
                dataSetIntId = OptionalLong.empty();
            }
        }

        return dataSetIntId;
    }

    /*
    Return the Cognite client.

    If the client isn't instantiated, it will be created according to the configured authentication options. After the
    initial instantiation, the client will be cached and reused.
     */
    private CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
            Preconditions.checkState(cdfProject.isPresent(),
                    "CDF project must be specified in the configuration.");
            // The client has not been instantiated yet
            if (clientId.isPresent() && clientSecret.isPresent() && aadTenantId.isPresent()) {
                cogniteClient = CogniteClient.ofClientCredentials(
                                clientId.get(),
                                clientSecret.get(),
                                TokenUrl.generateAzureAdURL(aadTenantId.get()),
                                Arrays.asList(authScopes))
                        .withProject(cdfProject.get())
                        .withBaseUrl(cdfHost);
            } else if (apiKey.isPresent()) {
                cogniteClient = CogniteClient.ofKey(apiKey.get())
                        .withProject(cdfProject.get())
                        .withBaseUrl(cdfHost);
            } else {
                String message = "Unable to instantiate the Cognite Client. No valid authentication configuration.";
                LOG.error(message);
                throw new Exception(message);
            }
        }

        return cogniteClient;
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
