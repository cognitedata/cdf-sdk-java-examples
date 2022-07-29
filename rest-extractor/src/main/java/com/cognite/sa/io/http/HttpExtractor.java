package com.cognite.sa.io.http;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.RawRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.SimpleTimer;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HttpExtractor {
    private static Logger LOG = LoggerFactory.getLogger(HttpExtractor.class);

    // source configs
    private static final String sourceUri = ConfigProvider.getConfig().getValue("source.uri", String.class);
    private static final String sourceHttpMethod = ConfigProvider.getConfig().getValue("source.method", String.class);
    private static final Optional<String> sourceBody =
            ConfigProvider.getConfig().getOptionalValue("source.body", String.class);
    private static final String sourceAuth =
            ConfigProvider.getConfig().getValue("source.authentication.protocol", String.class);
    private static final String sourceId =
            ConfigProvider.getConfig().getValue("source.authentication.id", String.class);
    private static final Optional<String> sourceSecret =
            ConfigProvider.getConfig().getOptionalValue("source.authentication.secret", String.class);
    private static final Optional<String> sourceSecretGcp =
            ConfigProvider.getConfig().getOptionalValue("source.authentication.secretGcp", String.class);

    // response configs
    private static final String responseFormat = ConfigProvider.getConfig().getValue("response.format", String.class);
    private static final String responsePath = ConfigProvider.getConfig().getValue("response.path", String.class);
    private static final String[] responseRowKey = ConfigProvider.getConfig().getValue("response.rowKey", String[].class);

    // target configs
    private static final String rawDb = ConfigProvider.getConfig().getValue("target.rawDb", String.class);
    private static final String rawTable = ConfigProvider.getConfig().getValue("target.rawTable", String.class);
    private static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("target.authentication.apiKey", String.class);
    private static final Optional<String> apiKeyGcp =
            ConfigProvider.getConfig().getOptionalValue("target.authentication.apiKeyGcp", String.class);

    // Metrics configs
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName = ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);

    // Metrics
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge processedRows = Gauge.build()
            .name("processed_rows_total").help("Total processed data objects/rows").register(collectorRegistry);
    static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    static final Gauge jobStageDurationSeconds = Gauge.build()
            .name("job_stage_duration_seconds").help("Job stage duration in seconds")
            .labelNames("stage").register(collectorRegistry);
    static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start time stamp").register(collectorRegistry);
    static final Gauge jobCompletionTimeStamp = Gauge.build()
            .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
    static final Gauge errorGauge = Gauge.build()
            .name("errors").help("Total errors when processing messages").register(collectorRegistry);

    // Common utility objects
    static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        try {
            new HttpExtractor().run();
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            System.exit(126); // exit code for permission problems, command cannot execute etc.
        }
    }

    public void run() throws Exception {
        LOG.info("Starting HTTP extractor...");

        // Prepare the metrics
        jobStartTimeStamp.setToCurrentTime();
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();
        Gauge.Timer jobStageReadTimer = jobStageDurationSeconds.labels("Read").startTimer();
        //startMetricsServer();

        LOG.info("Sending request to source uri: {}", sourceUri);
        HttpResponse<String> httpResponse = getHttpClient().send(buildHttpRequest(), HttpResponse.BodyHandlers.ofString());
        if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() < 300) {
            jobStageReadTimer.setDuration();
            LOG.info("Successfully received a response from the http source. Length {} characters",
                    httpResponse.body().length());

            Gauge.Timer jobStageWriteTimer = jobStageDurationSeconds.labels("Write").startTimer();
            List<RawRow> rowList = parseResponse(httpResponse);
            processedRows.set(rowList.size());
            LOG.info("Successfully parsed response. No rows: {}.", rowList.size());
            LOG.debug("Row 1: {}.", rowList.get(0).toString());

            LOG.info("Start writing rows to CDF raw. Db: {}. Table: {}",
                    rawDb,
                    rawTable);

            getCogniteClient().raw().rows().upsert(rowList, true);
            LOG.info("Finished writing rows to CDF raw.");

            jobStageWriteTimer.setDuration();
            jobDurationTimer.setDuration();
            jobCompletionTimeStamp.setToCurrentTime();
        } else {
            errorGauge.inc();
            LOG.warn("Error in the http response. Response status code: {}, Response body: {}",
                    httpResponse.statusCode(),
                    httpResponse.body());
        }

        // Push all our metrics
        pushMetrics();
    }

    /*
    Parse the string response body. Will identify the main payload (either a json object or an array of objects)
    and parse it into a (set of) raw row(s).
     */
    private List<RawRow> parseResponse(HttpResponse<String> response) throws Exception {
        LOG.debug("Start parsing http source response");
        JsonNode root = objectMapper.readTree(response.body());
        List<RawRow> rowList = new ArrayList<>();

        // Find the root of the payload
        if (!responsePath.isBlank()) {
            String[] pathSegments = responsePath.split("\\.");
            for (String path : pathSegments) {
                root = root.path(path);
            }
        }

        // Verify that the payload is valid.
        if (!(root.isObject() || root.isArray())) {
            String message = "Json payload must be an object or array.";
            LOG.error(message);
            throw new Exception(message);
        }

        if (root.isArray()) {
            for (JsonNode child: root) {
                rowList.add(parseRow(child));
            }
        } else {
            rowList.add(parseRow(root));
        }

        return rowList;
    }

    /*
    Parses a json object into a raw row:
     - Maps all object attributes to columns
     - Builds the row key
     - Adds db and table info
     */
    private RawRow parseRow(JsonNode root) throws Exception {
        Preconditions.checkNotNull(root,
                "Source payload is null or empty. Please check the source payload.");

        // the input must be a json object
        if (!root.isObject()) {
            String message = "Json payload must be an object per raw row.";
            LOG.error(message);
            throw new Exception(message);
        }

        // start parsing
        // add target raw db and table
        RawRow.Builder rowBuilder = RawRow.newBuilder()
                .setDbName(rawDb)
                .setTableName(rawTable);

        // parse the main payload into columns (struct)
        Struct.Builder columnsBuilder = Struct.newBuilder();
        JsonFormat.parser().merge(root.toString(), columnsBuilder);
        rowBuilder.setColumns(columnsBuilder);

        // add the row key
        String rowKey = "";
        for (String attribute : responseRowKey) {
            if (root.path(attribute).isValueNode()) {
                rowKey += root.path(attribute).asText("extractorDefaultKey");
            }
        }
        rowBuilder.setKey(rowKey);

        return rowBuilder.build();
    }

    /**
     * Builds the http request.
     *
     * Support for watermark must be added here.
     *
     * @return
     * @throws Exception
     */
    private HttpRequest buildHttpRequest() throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(sourceUri))
                .timeout(Duration.ofSeconds(20));

        if (sourceHttpMethod.equalsIgnoreCase("get")) {
            builder = builder
                    .GET();
        } else {
            throw new Exception("No valid http method specified: " + sourceHttpMethod);
        }

        return builder.build();
    }

    /**
     * Build the http client. Configure authentication here.
     * @return
     */
    private HttpClient getHttpClient() throws Exception {
        // get secret
        String tempSecret = "";
        if (sourceSecretGcp.isPresent()) {
            LOG.info("Getting source secret from GCP Secret Manager.");
            tempSecret = getGcpSecret(sourceSecretGcp.get().split("\\.")[0],
                    sourceSecretGcp.get().split("\\.")[1],
                    "latest");
        }
        if (sourceSecret.isPresent()) {
            LOG.info("Getting source secret from env variable or system properties.");
            tempSecret = sourceSecret.get();
        }
        // must reference secret via a final variable to be able to use it in the inner class below.
        final String secret = tempSecret;


        HttpClient client = HttpClient.newBuilder()
                .authenticator(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                sourceId,
                                secret.toCharArray());
                    }
                })
                .build();

        return client;
    }

    /*
    Builds the cognite client for writing data to raw.
     */
    private CogniteClient getCogniteClient() throws Exception {
        String key = "";
        if (apiKeyGcp.isPresent()) {
            LOG.info("Getting api key from GCP Secret Manager.");
            key = getGcpSecret(apiKeyGcp.get().split("\\.")[0],
                    apiKeyGcp.get().split("\\.")[1],
                    "latest");
        }
        if (apiKey.isPresent()) {
            LOG.info("Getting api key from env variable or system properties.");
            key = apiKey.get();
        }

        return CogniteClient.ofKey(key)
                .withBaseUrl("https://greenfield.cognitedata.com");
    }

    private void startMetricsServer() {
        DefaultExports.initialize();

        try {
            LOG.info("Starting Prometheus HTTP metrics server on port {}...", System.getenv("PORT"));
            HTTPServer server = new HTTPServer(Integer.parseInt(System.getenv("PORT")));
            LOG.info("Prometheus HTTP metrics server successfully started.");
        } catch (Exception e) {
            LOG.warn("Could not start metrics server: {}", e.toString());
        }
    }

    /*
    Push the current metrics to the push gateway.
     */
    private void pushMetrics() {
        if (pushGatewayUrl.isEmpty()) {
            LOG.warn("No metrics push gateway configured");
        }
        if (enableMetrics && pushGatewayUrl.isPresent()) {
            try {
                LOG.info("Pushing metrics to {}", pushGatewayUrl);
                PushGateway pg = new PushGateway(new URL(pushGatewayUrl.get())); //9091
                pg.pushAdd(collectorRegistry, metricsJobName);
            } catch (Exception e) {
                LOG.warn("Error when trying to push metrics: {}", e.toString());
            }
        }
    }

    /*
    Read secrets from GCP Secret Manager.

    Since we are using workload identity on GKE, we have to take into account that the identity metadata
    service for the pod may take a few seconds to initialize. Therefore the implicit call to get
    identity may fail if it happens at the very start of the pod. The workaround is to perform a
    retry.
     */
    private String getGcpSecret(String projectId, String secretId, String secretVersion) throws IOException {
        int maxRetries = 3;
        boolean success = false;
        IOException exception = null;
        String loggingPrefix = "getGcpSecret - ";
        String returnValue = "";

        for (int i = 0; i <= maxRetries && !success; i++) {
            // Initialize client that will be used to send requests.
            try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
                SecretVersionName name = SecretVersionName.of(projectId,
                        secretId, secretVersion);

                // Access the secret version.
                AccessSecretVersionRequest request =
                        AccessSecretVersionRequest.newBuilder().setName(name.toString()).build();
                AccessSecretVersionResponse response = client.accessSecretVersion(request);
                LOG.info(loggingPrefix + "Successfully read secret from GCP Secret Manager.");

                returnValue = response.getPayload().getData().toStringUtf8();
                success = true;
            } catch (IOException e) {
                String errorMessage = "Could not read secret from GCP secret manager. Will retry... " + e.getMessage();
                LOG.warn(errorMessage);
                exception = e;
            }
            if (!success) {
                // Didn't succeed in reading the secret. Pause the thread to wait for the metadata service to start.
                try {
                    Thread.sleep(1000l);
                } catch (Exception e) {
                    LOG.warn("Not able to pause thread: " + e);
                }
            }
        }

        if (!success) {
            // We didn't manage to read the secret
            LOG.error("Could not read secret from GCP secret manager: {}", exception.toString());
            throw exception;
        }

        return returnValue;
    }

}
