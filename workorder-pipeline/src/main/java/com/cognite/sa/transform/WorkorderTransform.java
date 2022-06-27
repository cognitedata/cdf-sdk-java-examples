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
import com.google.protobuf.util.Structs;
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

public class WorkorderTransform {
    private static Logger LOG = LoggerFactory.getLogger(WorkorderTransform.class);

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
    private static final String pipelineUpdateTimestampKey = "pipelineLastUpdatedTime";

    private static final String extIdPrefixWo = "sap.wo:";
    private static final String extIdPrefixWoOperation = "sap.wo.operation:";
    private static final String extIdPrefixWoConfirmation = "sap.wo.confirmation:";
    private static final String extIdPrefixWoObject = "sap.wo.object:";

    // global data structures
    private CogniteClient cogniteClient;
    private Map<String, Long> assetLookupMap;

    // Metrics
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge inputRows = Gauge.build()
            .name("input_rows_total").help("Total input rows").register(collectorRegistry);
    static final Gauge outputWo = Gauge.build()
            .name("output_workorders_total").help("Total output work orders").register(collectorRegistry);
    static final Gauge outputWoOperations = Gauge.build()
            .name("output_wo_operations_total").help("Total output wo operations").register(collectorRegistry);
    static final Gauge outputWoConfirmations = Gauge.build()
            .name("output_wo_confirmations_total").help("Total output wo confirmations").register(collectorRegistry);
    static final Gauge outputWoObjects = Gauge.build()
            .name("output_wo_objects_total").help("Total output wo objects").register(collectorRegistry);
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
            new WorkorderTransform().run();
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            System.exit(126); // exit code for permission problems, command cannot execute etc.
        }
    }

    public void run() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Starting the work order pipeline...");

        // Prepare the metrics
        jobStartTimeStamp.setToCurrentTime();
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();

        LOG.info("Start reading the workorder table: {}...", rawTable);
        List<Event> woEventsList = new ArrayList<>();
        Iterator<List<RawRow>> rawIterator = getCogniteClient().raw().rows().list(rawDb, rawTable);
        int inputRowsCounter = 0;
        int outputWoCounter = 0;
        int outputWoOperationCounter = 0;
        int outputWoConfirmationCounter = 0;
        int outputWoObjectsCounter = 0;
        while (rawIterator.hasNext()) {
            for (RawRow row : rawIterator.next()) {
                inputRowsCounter++;

                woEventsList.add(parseRowToWoEvent(row));
                outputWoCounter++;

                List<Event> woOperations = parseRowToWoOperationEvents(row);
                woEventsList.addAll(woOperations);
                outputWoOperationCounter += woOperations.size();

                List<Event> woConfirmations = parseRowToWoConfirmationEvents(row);
                woEventsList.addAll(woConfirmations);
                outputWoConfirmationCounter += woConfirmations.size();

                List<Event> woObjects = parseRowToWoObjectEvents(row);
                woEventsList.addAll(woObjects);
                outputWoObjectsCounter += woObjects.size();
            }
        }
        inputRows.set(inputRowsCounter);
        outputWo.set(outputWoCounter);
        outputWoOperations.set(outputWoOperationCounter);
        outputWoConfirmations.set(outputWoConfirmationCounter);
        outputWoObjects.set(outputWoObjectsCounter);

        LOG.info("Finished reading and parsing {} rows from {}. Duration {}.",
                woEventsList.size(),
                rawTable,
                Duration.between(startInstant, Instant.now()));

/*
        LOG.info("Start removing old workorders...");
        List<Event> woToDelete = new ArrayList<>();
        getCogniteClient().events()
                .list(Request.create()
                        .withFilterParameter("externalIdPrefix", "sap.wo"))
                .forEachRemaining(woToDelete::addAll);

        List<Item> deleteItems = woToDelete.stream()
                .map(event -> Item.newBuilder()
                        .setId(event.getId())
                        .build())
                .collect(Collectors.toList());

        getCogniteClient().events().delete(deleteItems);
        LOG.info("Finished removing {} workorders.", deleteItems.size());
*/


        LOG.info("Start writing the workorders...");
        List<Event> upserted = getCogniteClient().events().upsert(woEventsList);
        LOG.info("Finished writing the workorders.");

        jobDurationTimer.setDuration();
        jobCompletionTimeStamp.setToCurrentTime();

        LOG.info("Log pipeline results...");
        Event.Builder logEventBuilder = Event.newBuilder()
                .setExternalId("pipeline.result:" + Instant.now().toString())
                .setDescription("Work order pipeline results.")
                .setStartTime(startInstant.toEpochMilli())
                .setEndTime(Instant.now().toEpochMilli())
                .setType("pipeline-monitoring")
                .setSubtype("workorder-pipeline-results")
                .setSource("workorder-pipeline");

        upserted.stream()
                .filter(event -> event.getExternalId().startsWith("sap.wo.object:4000220:1901"))
                .limit(5)
                .forEach(event -> {
                    String value = String.format("Upserted event with id [%s], externalId [%s], description [%s], "
                            + "processingIndicator [%s]",
                            event.getId(),
                            event.getExternalId(),
                            event.getDescription(),
                            event.getMetadataOrDefault("ProcessingIndicator", "missing metadata"));
                    logEventBuilder.putMetadata(event.getExternalId(), value);
                    LOG.info(value);
                });

        upserted.stream()
                .filter(event -> event.getExternalId().startsWith("sap.wo.operation:4000021:"))
                .limit(5)
                .forEach(event -> {
                    String value = String.format("Upserted event with id [%s], externalId [%s], description [%s], "
                            + "ForecastedWorkQty [%s], ConfirmationTotalQuantity [%s]",
                            event.getId(),
                            event.getExternalId(),
                            event.getDescription(),
                            event.getMetadataOrDefault("ForecastedWorkQty", "missing metadata"),
                            event.getMetadataOrDefault("ConfirmationTotalQuantity", "missing metadata"));
                    logEventBuilder.putMetadata(event.getExternalId(), value);
                    LOG.info(value);
                });
        getCogniteClient().events().upsert(List.of(logEventBuilder.build()));
        LOG.info("Finished logging pipeline results");

        // Push all our metrics
        pushMetrics();
    }

    /*
    Parse the workorder raw row to WO Event.
     */
    private Event parseRowToWoEvent(RawRow row) throws Exception {
        final String loggingPrefix = "parseRowToWoEvent() - ";

        // Key columns
        final String extIdKey = "MaintenanceOrder";
        final String descriptionKey = "MaintenanceOrderDesc";
        final String startDateKey = "MaintOrdBasicStartDate";
        final String startTimeKey = "RequiredStartTime";
        final String endDateKey = "MaintOrdBasicEndDate";
        final String endTimeKey = "RequiredEndTime";

        final String notificationKey = "MaintenanceNotification";

        // Fixed values
        final String typeValue = "maintenanceObject";
        final String subtypeValue = "workorder";
        final String sourceValue = "SAP.Workorder";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Nested data
        final String operationKey = "to_MaintOrderOperation";
        final String operationKeyPath = "results.MaintenanceOrderOperation";
        final String operationKeyMetadataKey = "maintenanceOperations";

        final String confirmationsKey = "to_WorkOrderConfirmations";
        final String confirmationsKeyPath = "results.MaintOrderConf";
        final String confirmationsKeyMetadataKey = "maintenanceConfirmations";

        final String objectsKey = "to_WorkOrderObjects";
        final String objectsKeyPath = "results.ObjectNumber";
        final String objectsKeyMetadataKey = "maintenanceObjects";

        // Contextualization configuration
        final String assetReferenceKey = "TechnicalObjectLabel";
        final List<String> woObjectAssetReferencePath = List.of("results", "FunctionalLocation");
        final List<String> woOperationAssetReferenceKey = List.of("results", "TechnicalObjectLabel");

        Event.Builder eventBuilder = Event.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Add the mandatory fields
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            eventBuilder.setExternalId(extIdPrefixWo + columnsMap.get(extIdKey).getStringValue());
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
                        columnsMap.get(startDateKey).getStringValue(),
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

        // Parse all date fields from "/Date(124102348310483)/" to numeric milliseconds
        Map<String, String> parsedDates = metadata.entrySet().stream()
                .filter(entry -> Pattern.matches("\\/Date\\(\\d{12,}\\)\\/", entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> Instant.ofEpochMilli(parseEpochMs(entry.getValue()).get()).toString()));

        metadata.putAll(parsedDates);

        // Add fixed metadata
        metadata.put(sourceTableKey, rawTable);
        metadata.put(pipelineUpdateTimestampKey, Instant.now().toString());

        // Add the nested structures which require special, magic parsing
        if (columnsMap.containsKey(operationKey) && columnsMap.get(operationKey).hasStructValue()) {
            metadata.put(
                    operationKeyMetadataKey,
                    ParseStruct.parseStringDelimited(
                            columnsMap.get(operationKey).getStructValue(),
                            operationKeyPath,
                            ";"));
        }

        if (columnsMap.containsKey(confirmationsKey) && columnsMap.get(confirmationsKey).hasStructValue()) {
            metadata.put(
                    confirmationsKeyMetadataKey,
                    ParseStruct.parseStringDelimited(
                            columnsMap.get(confirmationsKey).getStructValue(),
                            confirmationsKeyPath,
                            ";"));
        }

        if (columnsMap.containsKey(objectsKey) && columnsMap.get(objectsKey).hasStructValue()) {
            metadata.put(
                    objectsKeyMetadataKey,
                    ParseStruct.parseStringDelimited(
                            columnsMap.get(objectsKey).getStructValue(),
                            objectsKeyPath,
                            ";"));
        }

        eventBuilder.putAllMetadata(metadata);

        /*
        Contextualization.
        - Add the top-level reference from the WO
        - Add the reference from the object list
        - Add the reference from the WO operations list
         */
        Set<String> assetReferences = new HashSet<>();
        if (columnsMap.containsKey(assetReferenceKey)
                && columnsMap.get(assetReferenceKey).hasStringValue()) {
            assetReferences.add(columnsMap.get(assetReferenceKey).getStringValue());
        }
        if (columnsMap.containsKey(objectsKey) && columnsMap.get(objectsKey).hasStructValue()) {
            assetReferences.addAll(ParseStruct.parseStringList(columnsMap.get(objectsKey), woObjectAssetReferencePath));
        }
        if (columnsMap.containsKey(operationKey) && columnsMap.get(operationKey).hasStructValue()) {
            assetReferences.addAll(ParseStruct.parseStringList(columnsMap.get(operationKey), woObjectAssetReferencePath));
        }
        for (String reference : assetReferences) {
            if (getAssetLookupMap().containsKey(reference)) {
                eventBuilder.addAssetIds(getAssetLookupMap().get(reference));
            }
        }
        if (assetReferences.isEmpty()) {
            LOG.warn(loggingPrefix + "Not able to link workorder [{}] to asset. Source input for WO column {}: {}",
                    eventBuilder.getExternalId(),
                    assetReferenceKey,
                    columnsMap.getOrDefault(assetReferenceKey, Values.of("null")));
        }

        return eventBuilder.build();
    }

    /*
    Parse the workorder raw row to WO operation Events.
     */
    private List<Event> parseRowToWoOperationEvents(RawRow row) throws Exception {
        final String loggingPrefix = "parseRowToWoOperationEvents() - ";
        List<Event> resultsList = new ArrayList<>();

        // Key columns
        final String[] extIdKey = {"MaintenanceOrder", "MaintenanceOrderOperation"};
        final String descriptionKey = "OperationDescription";
        final String startDateKey = "OpErlstSchedldExecStrtDte";
        final String startTimeKey = "OpErlstSchedldExecStrtTme";
        final String endDateKey = "OpLtstSchedldExecEndDte";
        final String endTimeKey = "OpLtstSchedldExecEndTme";

        // Fixed values
        final String typeValue = "maintenanceObject";
        final String subtypeValue = "workorderOperation";
        final String sourceValue = "SAP.WorkorderOperation";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Contextualization configuration
        final String assetReferenceKey = "TechnicalObjectLabel";

        // Check if the row contains nested WO operations
        List<Value> operationsList = row.getColumns()
                .getFieldsOrDefault("to_MaintOrderOperation", Values.of(Structs.of("foo", Values.of("bar"))))
                .getStructValue()
                .getFieldsOrDefault("results", Values.of(Collections.emptyList()))
                .getListValue().getValuesList();

        for (Value element : operationsList) {
            if (element.hasStructValue()) {
                Event.Builder eventBuilder = Event.newBuilder();
                Map<String, Value> columnsMap = element.getStructValue().getFieldsMap();

                // Add the mandatory externalId.
                if (columnsMap.containsKey(extIdKey[0])
                        && columnsMap.get(extIdKey[0]).hasStringValue()
                        && columnsMap.containsKey(extIdKey[1])
                        && columnsMap.get(extIdKey[1]).hasStringValue()) {
                    eventBuilder.setExternalId(extIdPrefixWoOperation
                            + columnsMap.get(extIdKey[0]).getStringValue()
                            + ":"
                            + columnsMap.get(extIdKey[1]).getStringValue());
                } else {
                    String message = String.format(loggingPrefix + "Could not parse field [%s].",
                            extIdKey[0] + ":" + extIdKey[1]);
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
                                columnsMap.get(startDateKey).getStringValue(),
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

                // Parse all date fields from "/Date(124102348310483)/" to numeric milliseconds
                Map<String, String> parsedDates = metadata.entrySet().stream()
                        .filter(entry -> Pattern.matches("\\/Date\\(\\d{12,}\\)\\/", entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                entry -> Instant.ofEpochMilli(parseEpochMs(entry.getValue()).get()).toString()));

                metadata.putAll(parsedDates);

                // Add fixed metadata
                metadata.put(sourceTableKey, rawTable);
                metadata.put(pipelineUpdateTimestampKey, Instant.now().toString());

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
                    LOG.warn(loggingPrefix + "Not able to link workorder operation to asset. Source input for column {}: {}",
                            assetReferenceKey,
                            columnsMap.getOrDefault(assetReferenceKey, Values.of("null")));
                }

                resultsList.add(eventBuilder.build());

            } else {
                LOG.warn(loggingPrefix + "WO Operation object is not a valid Struct/Json object.");
            }
        }

        return resultsList;
    }

    /*
    Parse the workorder raw row to WO confirmation Events.
     */
    private List<Event> parseRowToWoConfirmationEvents(RawRow row) throws Exception {
        final String loggingPrefix = "parseRowToWoConfirmationEvents() - ";
        List<Event> resultsList = new ArrayList<>();

        // Key columns
        final String[] extIdKey = {"MaintenanceOrder", "MaintOrderConf", "MaintOrderConfCntrValue"};
        final String descriptionKey = "OperationDescription";
        final String startDateKey = "OperationConfirmedStartDate";
        final String startTimeKey = "OperationConfirmedStartTime";
        final String endDateKey = "OperationConfirmedEndDate";
        final String endTimeKey = "OperationConfirmedEndTime";

        // Fixed values
        final String typeValue = "maintenanceObject";
        final String subtypeValue = "workorderConfirmation";
        final String sourceValue = "SAP.WorkorderConfirmation";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Contextualization configuration
        final String assetReferenceKey = "TechnicalObjectLabel";

        // Check if the row contains nested WO confirmations
        List<Value> operationsList = row.getColumns()
                .getFieldsOrDefault("to_WorkOrderConfirmations", Values.of(Structs.of("foo", Values.of("bar"))))
                .getStructValue()
                .getFieldsOrDefault("results", Values.of(Collections.emptyList()))
                .getListValue().getValuesList();

        for (Value element : operationsList) {
            if (element.hasStructValue()) {
                Event.Builder eventBuilder = Event.newBuilder();
                Map<String, Value> columnsMap = element.getStructValue().getFieldsMap();

                // Add the mandatory externalId.
                if (columnsMap.containsKey(extIdKey[0])
                        && columnsMap.get(extIdKey[0]).hasStringValue()
                        && columnsMap.containsKey(extIdKey[1])
                        && columnsMap.get(extIdKey[1]).hasStringValue()
                        && columnsMap.containsKey(extIdKey[2])
                        && columnsMap.get(extIdKey[2]).hasStringValue()) {
                    eventBuilder.setExternalId(extIdPrefixWoConfirmation
                            + columnsMap.get(extIdKey[0]).getStringValue()
                            + ":"
                            + columnsMap.get(extIdKey[1]).getStringValue()
                            + ":"
                            + columnsMap.get(extIdKey[2]).getStringValue());
                } else {
                    String message = String.format(loggingPrefix + "Could not parse field [%s].",
                            extIdKey[0] + ":" + extIdKey[1]);
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
                                columnsMap.get(startDateKey).getStringValue(),
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

                // Parse all date fields from "/Date(124102348310483)/" to numeric milliseconds
                Map<String, String> parsedDates = metadata.entrySet().stream()
                        .filter(entry -> Pattern.matches("\\/Date\\(\\d{12,}\\)\\/", entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                entry -> Instant.ofEpochMilli(parseEpochMs(entry.getValue()).get()).toString()));

                metadata.putAll(parsedDates);

                // Add fixed metadata
                metadata.put(sourceTableKey, rawTable);
                metadata.put(pipelineUpdateTimestampKey, Instant.now().toString());

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
                    LOG.warn(loggingPrefix + "Not able to link workorder operation to asset. Source input for column {}: {}",
                            assetReferenceKey,
                            columnsMap.getOrDefault(assetReferenceKey, Values.of("null")));
                }

                resultsList.add(eventBuilder.build());

            } else {
                LOG.warn(loggingPrefix + "WO Operation object is not a valid Struct/Json object.");
            }
        }

        return resultsList;
    }

    /*
    Parse the workorder raw row to a WO object / items Events.
     */
    private List<Event> parseRowToWoObjectEvents(RawRow row) throws Exception {
        final String loggingPrefix = "parseRowToWoObjectEvents() - ";
        List<Event> resultsList = new ArrayList<>();

        // Key columns
        final String[] extIdKey = {"WorkOrder", "ObjectNumber", "ObjectItem"};
        final String descriptionKey = "NotificationDesc";

        // Fixed values
        final String typeValue = "maintenanceObject";
        final String subtypeValue = "workorderItem";
        final String sourceValue = "SAP.WorkorderObject";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Contextualization configuration
        final String[] assetReferenceKeys = {"FunctionalLocation", "Equipment"};

        // Check if the row contains nested WO operations
        List<Value> operationsList = row.getColumns()
                .getFieldsOrDefault("to_WorkOrderObjects", Values.of(Structs.of("foo", Values.of("bar"))))
                .getStructValue()
                .getFieldsOrDefault("results", Values.of(Collections.emptyList()))
                .getListValue().getValuesList();

        for (Value element : operationsList) {
            if (element.hasStructValue()) {
                Event.Builder eventBuilder = Event.newBuilder();
                Map<String, Value> columnsMap = element.getStructValue().getFieldsMap();

                // Add the mandatory externalId.
                if (columnsMap.containsKey(extIdKey[0])
                        && columnsMap.get(extIdKey[0]).hasStringValue()
                        && columnsMap.containsKey(extIdKey[1])
                        && columnsMap.get(extIdKey[1]).hasStringValue()
                        && columnsMap.containsKey(extIdKey[2])) {
                    eventBuilder.setExternalId(extIdPrefixWoObject
                            + columnsMap.get(extIdKey[0]).getStringValue()
                            + ":"
                            + columnsMap.get(extIdKey[1]).getStringValue()
                            + ":"
                            + ParseValue.parseString(columnsMap.get(extIdKey[2])));
                } else {
                    String message = String.format(loggingPrefix + "Could not parse field [%s].",
                            extIdKey[0] + ":" + extIdKey[1] + ":" + extIdKey[2]);
                    LOG.error(message);
                    throw new Exception(message);
                }

                // Add optional fields
                if (columnsMap.containsKey(descriptionKey) && columnsMap.get(descriptionKey).hasStringValue()) {
                    eventBuilder.setDescription(columnsMap.get(descriptionKey).getStringValue());
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

                // Parse all date fields from "/Date(124102348310483)/" to numeric milliseconds
                Map<String, String> parsedDates = metadata.entrySet().stream()
                        .filter(entry -> Pattern.matches("\\/Date\\(\\d{12,}\\)\\/", entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                entry -> Instant.ofEpochMilli(parseEpochMs(entry.getValue()).get()).toString()));

                metadata.putAll(parsedDates);

                // Add fixed metadata
                metadata.put(sourceTableKey, rawTable);
                metadata.put(pipelineUpdateTimestampKey, Instant.now().toString());

                eventBuilder.putAllMetadata(metadata);

                /*
                Contextualization.
                - References: do a pure String lookup.
                 */
                for (String assetReference : assetReferenceKeys) {
                    if (columnsMap.containsKey(assetReference)
                            && columnsMap.get(assetReference).hasStringValue()
                            && getAssetLookupMap().containsKey(columnsMap.get(assetReference).getStringValue())) {
                        eventBuilder.addAssetIds(getAssetLookupMap().get(columnsMap.get(assetReference).getStringValue()));
                    }
                }
                if (eventBuilder.getAssetIdsCount() < 1) {
                    LOG.warn(loggingPrefix + "Not able to link workorder object to asset. Source input for column {}: {}",
                            assetReferenceKeys,
                            columnsMap.getOrDefault(assetReferenceKeys[0], Values.of("null")));
                }

                resultsList.add(eventBuilder.build());

            } else {
                LOG.warn(loggingPrefix + "WO object object is not a valid Struct/Json object.");
            }
        }

        return resultsList;
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

        // Read assets. The SDK client gives you an iterator back
        // that lets you "stream" batches of results.
        Iterator<List<Asset>> resultsIterator = getCogniteClient().assets().list(request);

        // Read the asset results, one batch at a time.
        resultsIterator.forEachRemaining(assets -> {
            for (Asset asset : assets) {
                // we break out the results batch and process each individual result.
                // In this case we want minimize the size of the asset collection
                // by removing the metadata bucket (we don't need all that metadata for
                // our processing).
                assetResults.add(asset.toBuilder().clearMetadata().build());
            }
        });

        return assetResults;
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

    /*
    Read secrets from GCP Secret Manager.

    Since we are using workload identity on GKE, we have to take into account that the identity metadata
    service for the pod may take a few seconds to initialize. Therefore, the implicit call to get
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
                    Thread.sleep(1000L);
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
