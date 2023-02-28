package com.cognite.sa.transform;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.Label;
import com.cognite.client.dto.RawRow;
import com.cognite.client.util.ParseValue;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.Value;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BuildAssetsTransform {
    private static Logger LOG = LoggerFactory.getLogger(BuildAssetsTransform.class);

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

    // raw source tables
    private static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String flocTable = ConfigProvider.getConfig().getValue("source.flocTable", String.class);
    private static final String eqTable = ConfigProvider.getConfig().getValue("source.eqTable", String.class);
    private static final String workCenterTable =
            ConfigProvider.getConfig().getValue("source.workCenterTable", String.class);

    // Metrics configs
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName = ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);

    // field keys/names, default values, default prefixes...
    private static final String sourceTableKey = "sourceTableRaw";

    private static final String extIdPrefixFloc = "sap.floc:";
    private static final String extIdPrefixEq = "sap.eq:";
    private static final String extIdPrefixWorkCenter = "sap.workcenter:";

    // A static root nodes for hosting dangling assets
    private static final Asset unassignedEqRoot = Asset.newBuilder()
            .setExternalId("synthetic:unassignedEquipmentRoot")
            .setName("Unassigned equipment")
            .setDescription("Synthetic root node for unassigned equipment.")
            .setSource("assetPipeline")
            .build();

    private static final Asset workCenterRoot = Asset.newBuilder()
            .setExternalId("synthetic:workCenterRoot")
            .setName("Work centers")
            .setDescription("Synthetic root node for work centers.")
            .setSource("assetPipeline")
            .build();

    // Metrics
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge inputFlocRows = Gauge.build()
            .name("input_rows_floc_total").help("Total input rows from the floc table").register(collectorRegistry);
    static final Gauge inputEqRows = Gauge.build()
            .name("input_rows_eq_total").help("Total input rows from the equipment table").register(collectorRegistry);
    static final Gauge inputWcRows = Gauge.build()
            .name("input_rows_wc_total").help("Total input rows from the work center table").register(collectorRegistry);
    static final Gauge outputAssets = Gauge.build()
            .name("output_assets_total").help("Total output assets").register(collectorRegistry);
    static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start time stamp").register(collectorRegistry);
    static final Gauge jobCompletionTimeStamp = Gauge.build()
            .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
    static final Gauge errorGauge = Gauge.build()
            .name("errors").help("Total errors when processing messages").register(collectorRegistry);

    private static CogniteClient cogniteClient = null;
    private static Map<String, Label> labelsMap = null;

    public static void main(String[] args) throws Exception {
        try {
            new BuildAssetsTransform().run();
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            System.exit(126); // exit code for permission problems, command cannot execute etc.
        }
    }

    public void run() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Starting asset builder pipeline...");
        // Prepare the metrics
        jobStartTimeStamp.setToCurrentTime();
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();

        List<Asset> assetList = new ArrayList<>();

        // add the synthetic root asset for non-linked equipment
        assetList.add(unassignedEqRoot);
        assetList.add(workCenterRoot);

        LOG.info("Start reading the floc table: {}...", flocTable);
        List<RawRow> flocRows = new ArrayList<>();
        getCogniteClient().raw().rows().list(rawDb, flocTable)
                .forEachRemaining(batch -> flocRows.addAll(batch));
        LOG.info("Finished reading {} rows from [{}]",
                flocRows.size(),
                flocTable);
        inputFlocRows.set(flocRows.size());

        LOG.info("Start parsing [{}] into assets", flocTable);
        for (RawRow row : flocRows) {
            assetList.add(parseFlocToAsset(row));
        }
        flocRows.clear();
        LOG.info("Finished parsing [{}] into assets", flocTable);

        LOG.info("Start reading the equipment table: {}...", eqTable);
        List<RawRow> eqRows = new ArrayList<>();
        getCogniteClient().raw().rows().list(rawDb, eqTable)
                .forEachRemaining(batch -> eqRows.addAll(batch));
        LOG.info("Finished reading {} rows from [{}]",
                eqRows.size(),
                eqTable);
        inputEqRows.set(eqRows.size());

        LOG.info("Start parsing [{}] into assets", eqTable);
        for (RawRow row : eqRows) {
            Asset equipment = parseEqToAsset(row);
            if (equipment.hasParentExternalId()) {
                assetList.add(equipment);
            } else {
                LOG.info("ExtId: [{}]. Equipment object does not have parent external id. Will not be included",
                        equipment.getExternalId());
                errorGauge.inc();
            }
        }
        eqRows.clear();
        LOG.info("Finished parsing [{}] into assets", eqTable);

        LOG.info("Start reading the work center table: {}...", workCenterTable);
        List<RawRow> wcRows = new ArrayList<>();
        getCogniteClient().raw().rows().list(rawDb, workCenterTable)
                .forEachRemaining(batch -> wcRows.addAll(batch));
        LOG.info("Finished reading {} rows from [{}]",
                wcRows.size(),
                workCenterTable);
        inputWcRows.set(wcRows.size());

        LOG.info("Start parsing [{}] into assets", workCenterTable);
        for (RawRow row : wcRows) {
            Asset workCenter = parseWorkCenterToAsset(row);
            if (workCenter.hasParentExternalId()) {
                assetList.add(workCenter);
            } else {
                LOG.info("ExtId: [{}]. Work center object does not have parent external id. Will not be included",
                        workCenter.getExternalId());
                errorGauge.inc();
            }
        }
        wcRows.clear();
        LOG.info("Finished parsing [{}] into assets", workCenterTable);

        LOG.info("Start synchronizing the asset hierarchy...");
        getCogniteClient().assets().synchronizeMultipleHierarchies(assetList);
        LOG.info("Finished synchronizing the asset hierarchy.");
        outputAssets.set(assetList.size());
        jobDurationTimer.setDuration();
        jobCompletionTimeStamp.setToCurrentTime();

        // Push all our metrics
        pushMetrics();
    }

    /*
    Parse the equipment raw row to Asset.
     */
    private Asset parseEqToAsset(RawRow row) throws Exception {
        final String loggingPrefix = "parseEqToAsset() - ";

        // Key columns
        final String extIdKey = "Equipment";
        final String parentExtIdKey = "Parentreference";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Labels config
        final String[] labelsList = {"label:equipment"};

        Asset.Builder assetBuilder = Asset.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Add the mandatory fields
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            assetBuilder.setExternalId(extIdPrefixEq + columnsMap.get(extIdKey).getStringValue());
            assetBuilder.setName(columnsMap.get(extIdKey).getStringValue());
            assetBuilder.setDescription(columnsMap.get(extIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    extIdKey);
            LOG.error(message);
            throw new Exception(message);
        }

        if (columnsMap.containsKey(parentExtIdKey)
                && columnsMap.get(parentExtIdKey).hasStringValue()
                && !columnsMap.get(parentExtIdKey).getStringValue().isBlank()) {
            assetBuilder.setParentExternalId(extIdPrefixFloc + columnsMap.get(parentExtIdKey).getStringValue());
        } else {
            assetBuilder.setParentExternalId(unassignedEqRoot.getExternalId());
            String message = String.format(loggingPrefix + "Row id %s: field [%s] does not exist or is blank.",
                    row.getKey(),
                    parentExtIdKey);
            LOG.debug(message);
        }

        // Add fixed values
        assetBuilder.setSource("SAP.Equipment");

        // Add labels. Check that the label exists in CDF before adding it as a reference.
        Arrays.stream(labelsList)
                .filter(getLabelsMap()::containsKey)
                .forEach(assetBuilder::addLabels);

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
        metadata.put(sourceTableKey, eqTable);

        assetBuilder.putAllMetadata(metadata);

        return assetBuilder.build();
    }

    /*
    Parse the floc raw row to Asset.
     */
    private Asset parseFlocToAsset(RawRow row) throws Exception {
        final String loggingPrefix = "parseFlocToAsset() - ";

        // Key columns
        final String extIdKey = "Functionallocation";
        final String parentExtIdKey = "Superiorfunctionallocation";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Labels config
        final String[] labelsList = {"label:functional-location"};

        Asset.Builder assetBuilder = Asset.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Add the mandatory fields
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            assetBuilder.setExternalId(extIdPrefixFloc + columnsMap.get(extIdKey).getStringValue());
            assetBuilder.setName(columnsMap.get(extIdKey).getStringValue());
            assetBuilder.setDescription(columnsMap.get(extIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    extIdKey);
            LOG.error(message);
            throw new Exception(message);
        }

        if (columnsMap.containsKey(parentExtIdKey)
                && columnsMap.get(parentExtIdKey).hasStringValue()
                && !columnsMap.get(parentExtIdKey).getStringValue().isBlank()) {
            assetBuilder.setParentExternalId(extIdPrefixFloc + columnsMap.get(parentExtIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Row id %s: field [%s] does not exist or is blank.",
                    row.getKey(),
                    parentExtIdKey);
            LOG.info(message);
        }

        // Add fixed values
        assetBuilder.setSource("SAP.Floc");

        // Add labels. Check that the label exists in CDF before adding it as a reference.
        Arrays.stream(labelsList)
                .filter(getLabelsMap()::containsKey)
                .forEach(assetBuilder::addLabels);

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
        metadata.put(sourceTableKey, flocTable);

        assetBuilder.putAllMetadata(metadata);

        return assetBuilder.build();
    }

    /*
    Parse the work center raw row to Asset.
     */
    private Asset parseWorkCenterToAsset(RawRow row) throws Exception {
        final String loggingPrefix = "parseWorkCenterToAsset() - ";

        // Key columns
        final String extIdKey = "WorkCenterInternalID";
        final String nameKey = "WorkCenter";
        final String descriptionKey = "WorkCenterDesc";

        // Include / exclude columns
        final String excludeColumnPrefix = "to_";
        List<String> excludeColumns = Arrays.asList("__metadata");

        // Labels config
        final String[] labelsList = {"label:work-center"};

        Asset.Builder assetBuilder = Asset.newBuilder();
        Map<String, Value> columnsMap = row.getColumns().getFieldsMap();

        // Just put all work centers under the synthetic work center root node
        assetBuilder.setParentExternalId(workCenterRoot.getExternalId());

        // Add the mandatory fields
        if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
            assetBuilder.setExternalId(extIdPrefixWorkCenter + columnsMap.get(extIdKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    extIdKey);
            LOG.error(message);
            throw new Exception(message);
        }

        if (columnsMap.containsKey(nameKey) && columnsMap.get(nameKey).hasStringValue()) {
            assetBuilder.setName(columnsMap.get(nameKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    nameKey);
            LOG.error(message);
            throw new Exception(message);
        }

        if (columnsMap.containsKey(descriptionKey) && columnsMap.get(descriptionKey).hasStringValue()) {
            assetBuilder.setDescription(columnsMap.get(descriptionKey).getStringValue());
        } else {
            String message = String.format(loggingPrefix + "Could not parse field [%s].",
                    descriptionKey);
            LOG.error(message);
            throw new Exception(message);
        }

        // Add fixed values
        assetBuilder.setSource("SAP.WorkCenter");

        // Add labels. Check that the label exists in CDF before adding it as a reference.
        Arrays.stream(labelsList)
                .filter(getLabelsMap()::containsKey)
                .forEach(assetBuilder::addLabels);

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
        metadata.put(sourceTableKey, workCenterTable);

        assetBuilder.putAllMetadata(metadata);

        return assetBuilder.build();
    }


    /*
    Builds a lookup map for floc:
    - Key is floc object id
    - Value is Functionallocation / externalId
     */
    private Map<String, String> buildFlocLookupMap(Collection<RawRow> row) {
        final String loggingPrefix = "buildFlocLookupMap() - ";

        final String extIdKey = "Functionallocation";
        final String objectIdKey = "Maintobjectinternalid";

        Map<String, String> keyMap = row.stream()
                .filter(rawRow -> rawRow.getColumns().containsFields(extIdKey)
                        && rawRow.getColumns().containsFields(objectIdKey))
                .map(rawRow -> rawRow.getColumns().getFieldsMap())
                .collect(Collectors.toMap(map -> map.get(objectIdKey).getStringValue(),
                        map -> map.get(extIdKey).getStringValue()));

        return keyMap;
    }

    private Map<String, Label> getLabelsMap() throws Exception {
        if (null == labelsMap) {
            labelsMap = new HashMap<>();
            getCogniteClient().labels()
                    .list()
                    .forEachRemaining(labelsBatch -> {
                        labelsMap.putAll(labelsBatch.stream()
                                .collect(Collectors.toMap(label -> label.getExternalId(), Function.identity())));
                    });
        }
        return labelsMap;
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
                            cdfProject,
                            clientId,
                            clientSecret,
                            TokenUrl.generateAzureAdURL(aadTenantId))
                    .withBaseUrl(cdfHost);
        }

        return cogniteClient;
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
