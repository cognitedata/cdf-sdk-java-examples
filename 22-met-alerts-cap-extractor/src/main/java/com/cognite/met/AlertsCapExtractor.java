package com.cognite.met;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.dto.RawRow;
import com.cognite.client.queue.UploadQueue;
import com.cognite.client.statestore.RawStateStore;
import com.cognite.client.statestore.StateStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class AlertsCapExtractor {
    private static Logger LOG = LoggerFactory.getLogger(AlertsCapExtractor.class);

    /*
    CDF project config. From config file / env variables.
     */
    private static final String cdfHost =
            ConfigProvider.getConfig().getValue("cognite.host", String.class);
    private static final String cdfProject =
            ConfigProvider.getConfig().getValue("cognite.project", String.class);
    private static final String clientId =
            ConfigProvider.getConfig().getValue("cognite.clientId", String.class);
    private static final String clientSecret =
            ConfigProvider.getConfig().getValue("cognite.clientSecret", String.class);
    private static final String aadTenantId =
            ConfigProvider.getConfig().getValue("cognite.azureADTenantId", String.class);
    private static final String[] authScopes =
            ConfigProvider.getConfig().getValue("cognite.scopes", String[].class);

    /*
    State store configuration. From config file
     */
    private static final Optional<String> stateStoreDb =
            ConfigProvider.getConfig().getOptionalValue("stateStore.raw.database", String.class);
    private static final Optional<String> stateStoreTable =
            ConfigProvider.getConfig().getOptionalValue("stateStore.raw.table", String.class);
    private static final Optional<String> stateStoreSaveInterval =
            ConfigProvider.getConfig().getOptionalValue("stateStore.raw.saveInterval", String.class);

    /*
    Source RSS config. From config file
     */
    private static final String sourceRawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String sourceRawTable =
            ConfigProvider.getConfig().getValue("source.rawTable", String.class);

    /*
    CDF.Raw target table configuration. From config file / env variables.
     */
    private static final String targetRawDb = ConfigProvider.getConfig().getValue("target.rawDb", String.class);
    private static final String targetRawTable =
            ConfigProvider.getConfig().getValue("target.rawTable", String.class);
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
    private static final Gauge noInvalidCapElementsGauge = Gauge.build()
            .name("job_no_invalid_cap_elements").help("Number of invalid CAP elements").register(collectorRegistry);

    /*
    Configuration settings--not from file
     */
    private static final String stateStoreExtId = "statestore:rss-met-alerts";
    private static final String lastUpdatedTimeKey = "source:lastUpdatedTime";

    // global data structures
    private static XmlMapper xmlMapper = new XmlMapper();
    private static CogniteClient cogniteClient;
    private static HttpClient httpClient;
    private static RawStateStore rawStateStore;

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
                        String.format("Upserted %d CAP items to CDF Raw.",
                                noElementsGauge.get()));
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
        LOG.info("Starting Met Alerts CAP extractor...");

        // Prepare the job start metrics
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();
        jobStartTimeStamp.setToCurrentTime();

        // Check if we have a state store configured. If yes, initialize it.
        getStateStore().ifPresent(stateStore ->
                {
                    try {
                        stateStore.load();
                        stateStore.start();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        LOG.info("Start reading RSS alerts from CDF Raw {}.{}...", sourceRawDb, sourceRawTable);
        // Read the RSS raw table
        List<RawRow> rssRawRows = new ArrayList<>();
        Request rssRequest = Request.create();
        if (getStateStore().isPresent()) {
            // We have a state store. Check the last updated timestamp and add it to the query as a filter.
            long lastUpdatedTime = getStateStore().get().getHigh(stateStoreExtId)
                    .orElse(1L);
            LOG.info("Previous state found in the state store. Will read RSS alerts with a minimum last updated time of {} "
                    + "-> {} [UTC]",
                    lastUpdatedTime,
                    Instant.ofEpochMilli(lastUpdatedTime).atOffset(ZoneOffset.UTC));
            rssRequest = rssRequest
                    .withFilterParameter("minLastUpdatedTime", lastUpdatedTime);
        }

        getCogniteClient().raw().rows().list(sourceRawDb, sourceRawTable, rssRequest)
                .forEachRemaining(rssRawRows::addAll);
        LOG.info("Finished reading {} RSS alerts", rssRawRows.size());

        // Parse the rss items to CAP URLs
        Map<String, Long> capUrlMap = rssRawRows.stream()
                .filter(rawRow -> rawRow.getColumns().containsFields("link"))
                .collect(Collectors.toMap(
                        row -> row.getColumns().getFieldsOrThrow("link").getStringValue(),
                        row -> row.getLastUpdatedTime()
                ));

        // Start the raw upload queue to prepare for uploading CAP alerts
        UploadQueue<RawRow, RawRow> rawRowUploadQueue = getCogniteClient().raw().rows().uploadQueue()
                .withPostUploadFunction(AlertsCapExtractor::postUpload);
        rawRowUploadQueue.start();

        LOG.info("Start reading CAP alerts from RSS item URLs...");
        // Read the CAP URLs
        for (Map.Entry<String, Long> capUrlEntry : capUrlMap.entrySet()) {
            LOG.debug("Sending request to source uri: {}", capUrlEntry.getKey());
            HttpResponse<String> httpResponse =
                    getHttpClient().send(buildHttpRequest(capUrlEntry.getKey()), HttpResponse.BodyHandlers.ofString());
            if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() < 300) {
                // We have a successful response. Parse the response body into a Raw row
                rawRowUploadQueue.put(parseRawRow(httpResponse.body(), httpResponse.uri().toString(), capUrlEntry.getValue()));
            } else {
                // Unsuccessful request. Most likely the CAP URL has expired
                noInvalidCapElementsGauge.inc();
                LOG.warn("CAP URL {} cannot be retrieved: {}", httpResponse.body());
            }
        }

        // Stop the upload queue. This will also perform a final upload.
        rawRowUploadQueue.stop();

        // Stop the state store. This will also store the final state
        getStateStore().ifPresent(stateStore -> stateStore.stop());

        LOG.info("Finished processing {} cap items. Duration {}",
                noElementsGauge.get(),
                Duration.between(startInstant, Instant.now()));
        jobDurationTimer.setDuration();

        // The job completion metric is only added to the registry after job success,
        // so that a previous success in the Pushgateway isn't overwritten on failure.
        Gauge jobCompletionTimeStamp = Gauge.build()
                .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
        jobCompletionTimeStamp.setToCurrentTime();
    }


    /**
     * Parse the CAP XML item to a raw row:
     *     - Convert the XML to a Json node tree to facilitate easier parsing logic.
     *     - Iterate over the collection of CAP information elements and identify the one with English language.
     *     - Add the entire English information element as Raw row columns.
     *     - Add basic lineage info to the Raw row.
     *     - Add source last updated time for delta load logic.
     *
     * @param capXml The source CAP XML.
     * @param sourceUri The source CAP URI.
     * @param lastUpdatedTime The RSS source last updated time (from the CDF Raw RSS row).
     * @return The {@link RawRow} representing the CAP info element
     * @throws Exception
     */
    public static RawRow parseRawRow(String capXml, String sourceUri, long lastUpdatedTime) throws Exception {
        final String loggingPrefix = "parseRawRow() - ";

        /*
        Key fields:
        - Source fields to use when parsing the source data.
        - Fields to add to the row for adding lineage etc.
         */
        final String sourceMainItemField = "info";
        final String sourceIdentifierField = "identifier";
        final String sourceLanguageField = "language";
        final String sourceLanguageValue = "en-GB";

        final String sourceUriKey = "source:uri";

        // The main Raw row builder objects we will populate with parsed data
        RawRow.Builder rowBuilder = RawRow.newBuilder()
                .setDbName(targetRawDb)
                .setTableName(targetRawTable);

        Struct.Builder structBuilder = Struct.newBuilder();

        // Use Jackson XML mapper to convert the XML into a node tree that is easier to parse
        JsonNode rootNode = xmlMapper.readTree(capXml.getBytes(StandardCharsets.UTF_8));

        // Add the mandatory fields
        // If a mandatory field is missing, you should flag it and handle that record specifically. Either by failing
        // the entire job, or putting the failed records in a "dead letter queue". In this case, we fail the job
        // by throwing an exception.
        if (rootNode.path(sourceIdentifierField).isTextual()) {
            rowBuilder.setKey(rootNode.path(sourceIdentifierField).textValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    sourceIdentifierField);
            LOG.error(message);
            throw new Exception(message);
        }
        /*
        Find the single info element with English content among the collection of CAP info elements. This is the main
        data payload. We add all the entire data payload as (nested) fields to the raw row columns.
         */
        if (rootNode.path(sourceMainItemField).isArray()) {
            for (JsonNode node : rootNode.path(sourceMainItemField)) {
                if (node.path(sourceLanguageField).isTextual()
                        && node.path(sourceLanguageField).textValue().equalsIgnoreCase(sourceLanguageValue)) {
                    JsonFormat.parser().merge(node.toString(), structBuilder);
                }
            }
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    sourceMainItemField);
            LOG.error(message);
            throw new Exception(message);
        }

        /*
        Add lineage info
         */
        structBuilder.putFields(sourceUriKey, Values.of(sourceUri));
        structBuilder.putFields(lastUpdatedTimeKey, Values.of(lastUpdatedTime));

        RawRow row = rowBuilder.setColumns(structBuilder).build();
        LOG.trace(loggingPrefix + "Parsed raw row: \n {}", row);
        return row;
    }

    /**
     * Builds the http request based on an input URI.
     *
     * @return
     * @throws Exception
     */
    private static HttpRequest buildHttpRequest(String uri) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(uri))
                .GET()
                .timeout(Duration.ofSeconds(20));

        return builder.build();
    }

    /*
    The post upload function. Will update the elements counter and update the state store (if configured)
    with the last updated time of the source.
     */
    private static void postUpload(List<RawRow> rawRows) {
        String loggingPrefix = "postUpload() -";
        if (rawRows.isEmpty()) {
            LOG.info(loggingPrefix + "No rows posted to Raw--will skip updating the state store.");
        }
        LOG.debug(loggingPrefix + "Submitted {} raw rows to CDF.", rawRows.size());
        LOG.debug(loggingPrefix + "Last updated time profile for first 5 rows: {}",
                rawRows.stream()
                        .limit(5)
                        .map(rawRow -> String.format("Key: %s - Last updated timestamp: %s",
                                rawRow.getKey(),
                                rawRow.getColumns().getFieldsOrDefault(lastUpdatedTimeKey, Values.ofNull())))
                        .toList());

        // Update the output elements counter
        noElementsGauge.inc(rawRows.size());

        // Find the most recent updated timestamp
        long lastUpdatedTime = rawRows.stream()
                .map(rawRow -> rawRow.getColumns().getFieldsOrDefault(lastUpdatedTimeKey, Values.of(0L)))
                .map(value -> value.getNumberValue())
                .mapToLong(number -> Math.round(number))
                .max()
                .orElse(0L);
        try {
            getStateStore().ifPresent(stateStore -> stateStore.expandHigh(stateStoreExtId, lastUpdatedTime));
            LOG.info("postUpload() - Posting to state store: {} - {}.", stateStoreExtId, lastUpdatedTime);
        } catch (Exception e) {
            LOG.warn("postUpload() - Unable to update the state store: {}", e.toString());
        }
    }

    /**
     * Build the http client. Configure authentication here.
     * @return
     */
    private static HttpClient getHttpClient() {
        if (null == httpClient) {
            httpClient = HttpClient.newBuilder()
                    .build();
        }

        return httpClient;
    }

    /*
    Return the Cognite client.

    If the client isn't instantiated, it will be created according to the configured authentication options. After the
    initial instantiation, the client will be cached and reused.
     */
    private static CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
            // The client has not been instantiated yet
            cogniteClient = CogniteClient.ofClientCredentials(
                            clientId,
                            clientSecret,
                            TokenUrl.generateAzureAdURL(aadTenantId),
                            Arrays.asList(authScopes))
                    .withProject(cdfProject)
                    .withBaseUrl(cdfHost);
        }

        return cogniteClient;
    }

    /*
    Return the state store (if configured)
     */
    private static Optional<StateStore> getStateStore() throws Exception {
        if (null == rawStateStore) {
            // Check if we have a state store config and instantiate the state store
            if (stateStoreDb.isPresent() && stateStoreTable.isPresent()) {
                LOG.info("State store defined in the configuration. Setting up Raw state store for {}.{}",
                        stateStoreDb.get(),
                        stateStoreTable.get());
                rawStateStore = RawStateStore.of(getCogniteClient(), stateStoreDb.get(), stateStoreTable.get());
                if (stateStoreSaveInterval.isPresent()) {
                    rawStateStore = rawStateStore
                            .withMaxCommitInterval(Duration.ofSeconds(Long.parseLong(stateStoreSaveInterval.get())));
                }
            }
        }

        return Optional.ofNullable(rawStateStore);
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
