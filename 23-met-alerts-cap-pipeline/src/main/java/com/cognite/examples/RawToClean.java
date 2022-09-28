package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
import com.cognite.client.util.ParseValue;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
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
    private static final String cdfProject =
            ConfigProvider.getConfig().getValue("cognite.project", String.class);
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
    Pipeline configuration
     */
    private static final String extIdPrefix = "source-name:";

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
    private static final Gauge noElementsContextualizedGauge = Gauge.build()
            .name("job_no_elements_contextualized").help("Number of contextualized elements").register(collectorRegistry);

    // global data structures
    private static CogniteClient cogniteClient;
    private static OptionalLong dataSetIntId;
    private static Map<String, Long> assetLookupMap;

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        boolean jobFailed = false;
        try {
            // Execute the main logic
            run();

            if (extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.SUCCESS,
                        String.format("Upserted %d events to CDF. %d events could be linked to assets.",
                                (int) noElementsGauge.get(),
                                (int) noElementsContextualizedGauge.get()));
            }
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
            jobFailed = true;
            if (extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.FAILURE,
                        String.format("Job failed: %s", e.getMessage()));
            }
        } finally {
            if (enableMetrics) {
                pushMetrics();
            }
            if (jobFailed) {
                System.exit(1); // container exit code for execution errors, etc.
            }
        }
    }

    /*
    The main logic to execute.
     */
    private static void run() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Starting raw to clean pipeline...");

        // Prepare the job start metrics
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();
        jobStartTimeStamp.setToCurrentTime();

        // Set up the reader for the raw table
        LOG.info("Starting to read the raw table {}.{}.",
                rawDb,
                rawTable);
        Iterator<List<RawRow>> rawResultsIterator = getCogniteClient().raw().rows().list(rawDb, rawTable);

        // Iterate through all rows in batches and write to clean. This will effectively "stream" through
        // the data so that we have ~constant memory usage no matter how large the data set is.
        while (rawResultsIterator.hasNext()) {
            // Temporary collection for hosting a single batch of parsed events.
            List<Event> events = new ArrayList<>();

            // Iterate through the individual rows in a single results batch and parse them to events.
            for (RawRow row : rawResultsIterator.next()) {
                Event event = parseRawRowToEvent(row);
                events.add(event);
            }

            // Upsert a batch of results to CDF
            getCogniteClient().events().upsert(events);
            noElementsGauge.inc(events.size());
        }

        LOG.info("Finished processing {} rows from raw. Duration {}",
                noElementsGauge.get(),
                Duration.between(startInstant, Instant.now()));
        jobDurationTimer.setDuration();

        // The job completion metric is only added to the registry after job success,
        // so that a previous success in the Pushgateway isn't overwritten on failure.
        Gauge jobCompletionTimeStamp = Gauge.build()
                .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
        jobCompletionTimeStamp.setToCurrentTime();
    }

    /*
    The main logic for parsing a Raw row to the target data structure--in this case an Event. Keep the code
    structured and readable for it to be easy to evolve and maintain.
     */
    private static Event parseRawRowToEvent(RawRow row) throws Exception {
        final String loggingPrefix = "parseRawRowToEvent() - ";

        /*
        Configuration section. Defines key (raw) columns and values for the parsing and transform logic.
         */
        // Key columns
        // These raw columns map to the event schema fields
        final String extIdKey = "RawExtIdColumn";
        final String descriptionKey = "RawDescriptionColumn";
        final String startDateTimeKey = "RawStartDateTimeColumn";
        final String endDataTimeKey = "RawEndDateTimeColumn";

        // Contextualization configuration
        final String assetReferenceKey = "RawAssetNameReferenceColumn";

        // Fixed values
        // Hardcoded values to add to the event schema fields
        final String typeValue = "event-type";
        final String subtypeValue = "event-subtype";
        final String sourceValue = "data-source-name";

        // Include / exclude columns
        // For filtering the entries to the metadata bucket
        final String excludeColumnPrefix = "exclude__";
        List<String> excludeColumns = List.of("exclude-column-a", "exclude-column-b", "exclude-column-c");

        /*
        The parsing logic.
         */
        Event.Builder eventBuilder = Event.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Add the mandatory fields
        // If a mandatory field is missing, you should flag it and handle that record specifically. Either by failing
        // the entire job, or putting the failed records in a "dead letter queue".
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            eventBuilder.setExternalId(extIdPrefix + columnsMap.get(extIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    extIdKey);
            LOG.error(message);
            throw new Exception(message);
        }
        if (columnsMap.containsKey(descriptionKey) && columnsMap.get(descriptionKey).hasStringValue()) {
            eventBuilder.setDescription(columnsMap.get(descriptionKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    descriptionKey);
            LOG.error(message);
            throw new Exception(message);
        }

        // Add optional fields. If an optional field is missing, no need to take any action (usually)
        if (columnsMap.containsKey(startDateTimeKey) && columnsMap.get(startDateTimeKey).hasNumberValue()) {
            eventBuilder.setStartTime((long) columnsMap.get(startDateTimeKey).getNumberValue());
        }
        if (columnsMap.containsKey(endDataTimeKey) && columnsMap.get(endDataTimeKey).hasNumberValue()) {
            eventBuilder.setEndTime((long) columnsMap.get(endDataTimeKey).getNumberValue());
        }

        // Add fixed values
        eventBuilder
                .setSource(sourceValue)
                .setType(typeValue)
                .setSubtype(subtypeValue);

        // Add fields to metadata based on the exclusion filters
        Map<String, String> metadata = columnsMap.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(excludeColumnPrefix))
                .filter(entry -> !excludeColumns.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ParseValue.parseString(entry.getValue())));

        // Add basic lineage info
        metadata.put("dataSource",
                String.format("CDF Raw: %s.%s.%s", row.getDbName(), row.getTableName(), row.getKey()));

        // Don't forget to add the metadata to the event object
        eventBuilder.putAllMetadata(metadata);

        // If a target dataset has been configured, add it to the event object
        if (getDataSetIntId().isPresent()) {
            eventBuilder.setDataSetId(dataSetIntId.getAsLong());
        }

        /*
        Contextualization.
        - Do a pure name-based, exact match asset lookup.
        - Log a successful contextualization operation as a metric.
         */
        if (columnsMap.containsKey(assetReferenceKey)
                && columnsMap.get(assetReferenceKey).hasStringValue()
                && getAssetLookupMap().containsKey(columnsMap.get(assetReferenceKey).getStringValue())) {
            eventBuilder.addAssetIds(getAssetLookupMap().get(columnsMap.get(assetReferenceKey).getStringValue()));
            noElementsContextualizedGauge.inc();
        } else {
            LOG.warn(loggingPrefix + "Not able to link event to asset. Source input for column {}: {}",
                    assetReferenceKey,
                    columnsMap.getOrDefault(assetReferenceKey, Values.of("null")));
        }

        // Build the event object
        return eventBuilder.build();
    }

    /*
    Return the data set internal id.

    If the data set external id has been configured, this method will translate this to the corresponding
    internal id.
     */
    private static OptionalLong getDataSetIntId() throws Exception {
        if (null == dataSetIntId) {
            if (targetDataSetExtId.isPresent()) {
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
    Return the asset lookup map. The lookup map is used for linking the events to assets. In this example, the
    lookup key is the asset name.
     */
    private static Map<String, Long> getAssetLookupMap() throws Exception {
        if (null == assetLookupMap) {
            LOG.info("Start reading the assets from CDF...");
            assetLookupMap = readAssets().stream()
                    .collect(Collectors.toMap(Asset::getName, Asset::getId));

            LOG.info("Finished reading {} assets from CDF.",
                    assetLookupMap.size());
        }

        return assetLookupMap;
    }

    /*
    Read the assets collection and minimize the asset objects.
     */
    private static List<Asset> readAssets() throws Exception {
        List<Asset> assetResults = new ArrayList<>();

        // Read all assets. The SDK client gives you an iterator back
        // that lets you "stream" batches of results.
        Iterator<List<Asset>> resultsIterator = getCogniteClient().assets().list();

        // Read the asset results, one batch at a time.
        resultsIterator.forEachRemaining(assets -> {
            for (Asset asset : assets) {
                // we break out the results batch and process each individual result.
                // In this case we want to minimize the size of the asset collection
                // by removing the metadata bucket (we don't need all that metadata for contextualization).
                assetResults.add(asset.toBuilder().clearMetadata().build());
            }
        });

        return assetResults;
    }

    /*
    Return the Cognite client.

    If the client isn't instantiated, it will be created according to the configured authentication options. After the
    initial instantiation, the client will be cached and reused.
     */
    private static CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
            // The client has not been instantiated yet
            if (clientId.isPresent() && clientSecret.isPresent() && aadTenantId.isPresent()) {
                cogniteClient = CogniteClient.ofClientCredentials(
                                clientId.get(),
                                clientSecret.get(),
                                TokenUrl.generateAzureAdURL(aadTenantId.get()),
                                Arrays.asList(authScopes))
                        .withProject(cdfProject)
                        .withBaseUrl(cdfHost);
            } else if (apiKey.isPresent()) {
                cogniteClient = CogniteClient.ofKey(apiKey.get())
                        .withProject(cdfProject)
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
    Creates an extraction pipeline run and writes it to Cognite Data Fusion.
     */
    private static boolean writeExtractionPipelineRun(ExtractionPipelineRun.Status status, String message) {
        boolean writeSuccess = false;
        if (extractionPipelineExtId.isPresent()) {
            try {
                ExtractionPipelineRun pipelineRun = ExtractionPipelineRun.newBuilder()
                            .setExternalId(extractionPipelineExtId.get())
                            .setCreatedTime(Instant.now().toEpochMilli())
                            .setStatus(status)
                            .setMessage(message)
                            .build();

                LOG.info("Writing extraction pipeline run with status: {}", pipelineRun);
                getCogniteClient().extractionPipelines().runs().create(List.of(pipelineRun));
                writeSuccess = true;
            } catch (Exception e) {
                LOG.warn("Error when trying to create extraction pipeline run: {}", e.toString());
            }
        } else {
            LOG.warn("Extraction pipeline external id is not configured. Cannot create pipeline run.");
        }

        return writeSuccess;
    }

    /*
    Push the current metrics to the push gateway.
     */
    private static boolean pushMetrics() {
        boolean isSuccess = false;
        if (pushGatewayUrl.isPresent()) {
            try {
                LOG.info("Pushing metrics to {}", pushGatewayUrl);
                PushGateway pg = new PushGateway(new URL(pushGatewayUrl.get())); //9091
                pg.pushAdd(collectorRegistry, metricsJobName);
                isSuccess = true;
            } catch (Exception e) {
                LOG.warn("Error when trying to push metrics: {}", e.toString());
            }
        } else {
            LOG.warn("No metrics push gateway configured. Cannot push the metrics.");
        }

        return isSuccess;
    }
}
