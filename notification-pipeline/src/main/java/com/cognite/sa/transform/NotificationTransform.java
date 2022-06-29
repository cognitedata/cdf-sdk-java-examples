package com.cognite.sa.transform;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.RawRow;
import com.cognite.client.util.ParseStruct;
import com.cognite.client.util.ParseValue;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NotificationTransform {
    private static Logger LOG = LoggerFactory.getLogger(NotificationTransform.class);

    // cdf auth config
    private static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("cdf.authentication.apiKey", String.class);
    private static final Optional<String> apiKeyGcp =
            ConfigProvider.getConfig().getOptionalValue("cdf.authentication.apiKeyGcp", String.class);

    // raw source tables
    private static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String rawTable =
            ConfigProvider.getConfig().getValue("source.table", String.class);

    // Metrics configs
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName = ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);

    // field keys/names, default values, default prefixes...
    private static final String sourceTableKey = "sourceTableRaw";
    private static final String extIdPrefix = "sap.notification:";

    // global data structures
    private CogniteClient cogniteClient;
    private Map<String, Long> assetLookupMap;

    // Metrics
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge inputRows = Gauge.build()
            .name("input_rows_total").help("Total input rows").register(collectorRegistry);
    static final Gauge outputEvents = Gauge.build()
            .name("output_events_total").help("Total output events").register(collectorRegistry);
    static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start time stamp").register(collectorRegistry);
    static final Gauge jobCompletionTimeStamp = Gauge.build()
            .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
    static final Gauge errorGauge = Gauge.build()
            .name("errors").help("Total errors when processing messages").register(collectorRegistry);

    public static void main(String[] args) throws Exception {
        try {
            new NotificationTransform().run();
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            System.exit(126); // exit code for permission problems, command cannot execute etc.
        }
    }

    public void run() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Starting notification pipeline...");

        // Prepare the metrics
        jobStartTimeStamp.setToCurrentTime();
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();

        LOG.info("Start reading the notification table: {}...", rawTable);
        List<Event> notificationList = new ArrayList<>();
        Iterator<List<RawRow>> rawIterator = getCogniteClient().raw().rows().list(rawDb, rawTable);
        int inputRowCounter = 0;
        while (rawIterator.hasNext()) {
            for (RawRow row : rawIterator.next()) {
                inputRowCounter++;
                notificationList.add(parseNotificationRowToEvent(row));
            }
        }
        inputRows.set(inputRowCounter);
        outputEvents.set(notificationList.size());

        LOG.info("Finished reading and parsing {} rows from {}. Duration {}.",
                notificationList.size(),
                rawTable,
                Duration.between(startInstant, Instant.now()));

        LOG.info("Start writing the notifications...");
        List<Event> upserted = getCogniteClient().events().upsert(notificationList);
        jobDurationTimer.setDuration();
        jobCompletionTimeStamp.setToCurrentTime();
        LOG.info("Finished writing the notifications.");

        // Push all our metrics
        pushMetrics();
    }

    /*
    Parse the notification raw row to Event.
     */
    private Event parseNotificationRowToEvent(RawRow row) throws Exception {
        final String loggingPrefix = "parseNotificationRowToEvent() - ";

        // Key columns
        final String extIdKey = "MaintenanceNotification";
        final String descriptionKey = "NotificationText";
        final String startDateKey = "RequiredStartDate";
        final String startTimeKey = "RequiredStartTime";
        final String endDateKey = "RequiredEndDate";
        final String endTimeKey = "RequiredEndTime";

        // Fixed values
        final String typeValue = "maintenanceObject";
        final String subtypeValue = "notification";
        final String sourceValue = "SAP.Notification";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Nested data
        final String systemStatusKey = "to_SystemStatus";
        final String systemStatusCodePath = "results.StatusCode";
        final String systemStatusCodeMetadataKey = "systemStatusCode";
        final String systemStatusShortNamePath = "results.StatusShortName";
        final String systemStatusShortNameMetadataKey = "systemStatusShortName";

        // Contextualization configuration
        final String assetReferenceKey = "TechnicalObjectLabel";

        // Matchers for date columns
        Pattern datePattern = Pattern.compile("\\d{10,}");

        Event.Builder eventBuilder = Event.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Add the mandatory fields
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            eventBuilder.setExternalId(extIdPrefix + columnsMap.get(extIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    extIdKey);
            LOG.error(message);
            throw new Exception(message);
        }

        // Add optional fields
        if (columnsMap.containsKey(descriptionKey) && columnsMap.get(descriptionKey).hasStringValue()) {
            eventBuilder.setDescription(columnsMap.get(descriptionKey).getStringValue());
        }
        if (columnsMap.containsKey(startDateKey) && columnsMap.get(startDateKey).hasStringValue()) {
            try {
                Optional<Long> epochMs = parseEpochMs(columnsMap.get(startDateKey).getStringValue());
                epochMs.ifPresent(eventBuilder::setStartTime);
            } catch (NumberFormatException e) {
                LOG.warn(loggingPrefix + "Could not parse value [{}] from column [{}] into numeric milliseconds.",
                        columnsMap.get(endDateKey).getStringValue(),
                        endDateKey);
            }
        }
        if (columnsMap.containsKey(endDateKey) && columnsMap.get(endDateKey).hasStringValue()) {
            try {
                Optional<Long> epochMs = parseEpochMs(columnsMap.get(endDateKey).getStringValue());
                epochMs.ifPresent(eventBuilder::setEndTime);
            } catch (NumberFormatException e) {
                LOG.warn(loggingPrefix + "Could not parse value [{}] from column [{}] into numeric milliseconds.",
                        columnsMap.get(endDateKey).getStringValue(),
                        endDateKey);
            }
        }

        // Add fixed values
        eventBuilder
                .setSource(sourceValue)
                .setType(typeValue)
                .setSubtype(subtypeValue);

        // Add fields to metadata based on the exclude filters
        Map<String, String> metadata = columnsMap.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(excludeColumnPrefix))
                .filter(entry -> !excludeColumns.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ParseValue.parseString(entry.getValue())));

        // Parse all metadata date fields from "/Date(124102348310483)/" to numeric milliseconds
        Map<String, String> parsedDates = metadata.entrySet().stream()
                .filter(entry -> Pattern.matches("\\/Date\\(\\d{12,}\\)\\/", entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> Instant.ofEpochMilli(parseEpochMs(entry.getValue()).get()).toString()));

        metadata.putAll(parsedDates);

        // Add fixed metadata
        metadata.put(sourceTableKey, rawTable);

        // Add the nested structures which require special, magic parsing
        if (columnsMap.containsKey(systemStatusKey) && columnsMap.get(systemStatusKey).hasStructValue()) {
            metadata.put(
                    systemStatusCodeMetadataKey,
                    ParseStruct.parseStringDelimited(
                            columnsMap.get(systemStatusKey).getStructValue(),
                            systemStatusCodePath,
                            ";"));

            metadata.put(
                    systemStatusShortNameMetadataKey,
                    ParseStruct.parseStringDelimited(
                            columnsMap.get(systemStatusKey).getStructValue(),
                            systemStatusShortNamePath,
                            ";"));
        }

        eventBuilder.putAllMetadata(metadata);

        /*
        Contextualization.
        - Floc references: do a pure String lookup.
        - Eq references: need to strip out leading zeros before matching.
         */
        if (columnsMap.containsKey(assetReferenceKey)
                && columnsMap.get(assetReferenceKey).hasStringValue()
                && getAssetLookupMap().containsKey(columnsMap.get(assetReferenceKey).getStringValue())) {
            eventBuilder.addAssetIds(getAssetLookupMap().get(columnsMap.get(assetReferenceKey).getStringValue()));
        } else {
            LOG.warn(loggingPrefix + "Not able to link notification to asset. Source input for column {}: {}",
                    assetReferenceKey,
                    columnsMap.getOrDefault(assetReferenceKey, Values.of("null")));
        }


        return eventBuilder.build();
    }

    /*
    Read the assets collection and minimize the asset objects.

    Only assets with labels functional location and equipment are valid contextualization targets.
     */
    private List<Asset> readAssets() throws Exception {
        List<Asset> assetResults = new ArrayList<>();

        Request request = Request.create()
                .withFilterParameter("labels", Map.of("containsAny", List.of(
                        Map.of("externalId", "label:functional-location"),
                        Map.of("externalId", "label:equipment"))));

        // Read all assets. The SDK client gives you an iterator back
        // that lets you "stream" batches of results.
        Iterator<List<Asset>> resultsIterator = getCogniteClient().assets().list(request);

        // Read the asset results, one batch at a time.
        resultsIterator.forEachRemaining(assets -> {
            for (Asset asset : assets) {
                // we break out the results batch and process each individual result.
                // In this case we want minimize the size of the asset collection
                // by removing the metadata bucket (we don't need all that metadata for
                // our processing.
                assetResults.add(asset.toBuilder().clearMetadata().build());
            }
        });

        return assetResults;
    }

    private Optional<Long> parseEpochMs(String dateTimeString) throws NumberFormatException {
        // Matchers for date columns
        Pattern datePattern = Pattern.compile("\\d{10,}");
        Matcher m = datePattern.matcher(dateTimeString);
        Optional<Long> returnValue = Optional.empty();
        if (m.find()) {
            String dateInMs = m.group();
            returnValue = Optional.of(Long.parseLong(dateInMs));
        }

        return returnValue;
    }

    private Map<String, Long> getAssetLookupMap() throws Exception {
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
    Builds the cognite client.
     */
    private CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
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

            cogniteClient = CogniteClient.ofKey(key)
                    .withBaseUrl("https://greenfield.cognitedata.com")
                    .withClientConfig(ClientConfig.create()
                            .withUpsertMode(UpsertMode.REPLACE));
        }

        return cogniteClient;
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

}
