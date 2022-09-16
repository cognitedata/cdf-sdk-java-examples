package com.cognite.met;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.dto.RawRow;
import com.cognite.client.queue.UploadQueue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

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
    Source RSS config
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

    // global data structures
    private static XmlMapper xmlMapper = new XmlMapper();
    private static CogniteClient cogniteClient;
    private static HttpClient httpClient;

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
                        String.format("Upserted %d RSS items to CDF Raw.",
                                noElementsGauge.get()));
            }
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
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

        LOG.info("Start reading CAP alerts from CDF Raw {}.{}...", targetRawDb, targetRawTable);
        // Read the CAP raw table. Must check that the table exists
        List<String> rawDbs = new ArrayList<>();
        List<String> rawTables = new ArrayList<>();
        List<RawRow> capRawRows = new ArrayList<>();

        getCogniteClient().raw().databases().list()
                .forEachRemaining(rawDbs::addAll);
        if (rawDbs.contains(targetRawDb)) {
            getCogniteClient().raw().tables().list(targetRawDb)
                    .forEachRemaining(rawTables::addAll);
            if (rawTables.contains(targetRawTable)) {
                getCogniteClient().raw().rows().list(targetRawDb, targetRawTable)
                        .forEachRemaining(capRawRows::addAll);
            }
        }
        LOG.info("Read {} CAP alerts", capRawRows.size());

        LOG.info("Start reading RSS alerts from CDF Raw {}.{}...", sourceRawDb, sourceRawTable);
        // Read the RSS raw table
        List<RawRow> rssRawRows = new ArrayList<>();
        getCogniteClient().raw().rows().list(sourceRawDb, sourceRawTable)
                .forEachRemaining(rssRawRows::addAll);
        LOG.info("Read {} RSS alerts", rssRawRows.size());

        // Parse the rss items to CAP URLs
        List<String> capUrls = rssRawRows.stream()
                .filter(rawRow -> rawRow.getColumns().containsFields("link"))
                .map(row -> row.getColumns().getFieldsOrThrow("link").getStringValue())
                .toList();

        // Start the raw upload queue
        UploadQueue<RawRow, RawRow> rawRowUploadQueue = getCogniteClient().raw().rows().uploadQueue()
                .withPostUploadFunction(rawRows -> noElementsGauge.inc(rawRows.size()));
        rawRowUploadQueue.start();

        LOG.info("Start reading CAP alerts from RSS item URLs...");
        // Read the CAP URLs
        for (String capUrl : capUrls) {
            LOG.debug("Sending request to source uri: {}", capUrl);
            HttpResponse<String> httpResponse =
                    getHttpClient().send(buildHttpRequest(capUrl), HttpResponse.BodyHandlers.ofString());
            if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() < 300) {
                rawRowUploadQueue.put(parseRawRow(httpResponse.body()));
            } else {
                LOG.warn("CAP URL {} cannot be retrieved: {}", httpResponse.body());
            }
        }

        // Stop the upload queue. This will also perform a final upload.
        rawRowUploadQueue.stop();

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

    /*
    Parse the CAP XML item to a raw row.
     */
    public static RawRow parseRawRow(String capXml) throws Exception {
        final String loggingPrefix = "parseRawRow() - ";

        /*
        Key fields
         */
        final String mainItemField = "info";
        final String identifierField = "identifier";
        final String languageField = "language";
        final String languageValue = "en-GB";

        RawRow.Builder rowBuilder = RawRow.newBuilder()
                .setDbName(targetRawDb)
                .setTableName(targetRawTable);

        Struct.Builder structBuilder = Struct.newBuilder();
        JsonNode rootNode = xmlMapper.readTree(capXml.getBytes(StandardCharsets.UTF_8));

        // Add the mandatory fields
        // If a mandatory field is missing, you should flag it and handle that record specifically. Either by failing
        // the entire job, or putting the failed records in a "dead letter queue".
        if (rootNode.path(identifierField).isTextual()) {
            rowBuilder.setKey(rootNode.path(identifierField).textValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    identifierField);
            LOG.error(message);
            throw new Exception(message);
        }
        /*
        Find the info element with English content
         */
        if (rootNode.path(mainItemField).isArray()) {
            for (JsonNode node : rootNode.path(mainItemField)) {
                if (node.path(languageField).isTextual()
                        && node.path(languageField).textValue().equalsIgnoreCase(languageValue)) {
                    JsonFormat.parser().merge(node.toString(), structBuilder);
                }
            }
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    mainItemField);
            LOG.error(message);
            throw new Exception(message);
        }

        RawRow row = rowBuilder.setColumns(structBuilder).build();
        LOG.debug(loggingPrefix + "Parsed raw row: \n {}", row);
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

    /**
     * Build the http client. Configure authentication here.
     * @return
     */
    private static HttpClient getHttpClient() throws Exception {
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
