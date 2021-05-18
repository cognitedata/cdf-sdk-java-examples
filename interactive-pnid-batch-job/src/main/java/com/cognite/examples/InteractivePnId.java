package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.dto.*;
import com.cognite.client.util.Partition;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class InteractivePnId {
    private static Logger LOG = LoggerFactory.getLogger(InteractivePnId.class);
    private final static String baseURL = "https://api.cognitedata.com";
    private final static String fileNameSuffixRegEx = "\\.[a-zA-Z]{1,4}$";
    private static String appIdentifier = "my-interactive-pnid-pipeline";

    /* Pipeline config parameters */
    // filters to select the source P&IDs
    private final static String fileSourceDataSetExternalId = "dataset:d2-lci-files";
    private final static String fileSourceFilterMetadataKey = "DOCUMENT TYPE CODE";
    private final static String fileSourceFilterMetadataValue = "XB";

    // filters to select the assets to use as lookup values for the P&ID service
    private final static String assetDataSetExternalId = "dataset:aveva-net-assets";

    // the target data set for the interactive P&IDs
    private final static String fileTargetDataSetExternalId = "dataset:d2-interactive-pnids";

    //
    private final static String fileTechLocationMetadataKey = "document:abp_tech_location";

    // Contextualization metadata
    public final static String contextAgentKey = "contextAgent";
    public final static String contextAlgorithmKey = "contextAlgorithm";
    private final static String originDocExtIdKey =  "contextInteractivePnIdSourceDocExtId";
    private final static String originDocUploadedTimeKey =  "contextInteractivePnIdSourceDocUploadedTime";
    private final static String contextAgent = "Interactive P&ID pipeline";
    private final static String contextAlgorithm = "Copy source file asset links";

    private static CogniteClient client = null;
    private static Map<String, Long> dataSetExternalIdMap = null;

    public static void main(String[] args) throws Exception {
        Instant startInstant = Instant.now();
        int countFiles = 0;

        // Read the assets which we'll use as a basis for looking up entities in the P&ID
        LOG.info("Start downloading assets.");
        List<Asset> assetsResults = readAssets();
        LOG.info("Finished downloading {} assets. Duration: {}",
                assetsResults.size(),
                Duration.between(startInstant, Instant.now()));

        /*
        We need to construct the actual lookup entities. This is input to the
        P&ID service and guides how entities are detected in the P&ID.
        For example, if you want to take spelling variations into account, then this is
        the place you would add that configuration.
         */
        LOG.info("Start building detect entities struct.");
        List<Struct> matchToEntities = new ArrayList<>();
        for (Asset asset : assetsResults) {
            // The default text to search for
            Value entityName = Values.of(asset.getName());

            // check if the asset matches the condition for using spelling variations
            if (asset.getName().length() > 10) {
                List<Value> spellingVariations = new ArrayList<>();
                spellingVariations.add(entityName); // the default search text is one of the entries

                // add the second spelling variation
                spellingVariations.add(Values.of(asset.getName().substring(3)));

                // combine the variations into the new "Value" representation
                entityName = Values.of(spellingVariations);
            }

            // build the match entity
            Struct matchTo = Struct.newBuilder()
                    .putFields("externalId", Values.of(asset.getExternalId().getValue()))
                    .putFields("id", Values.of(asset.getId().getValue()))
                    .putFields("name", entityName)
                    .putFields("resourceType", Values.of("Asset"))
                    .build();

            matchToEntities.add(matchTo);
        }
        LOG.info("Finished building detect entities struct. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        /*
        Build the list of files to run through the interactive P&ID process. The P&ID service has
        some restrictions on which file types it supports, so you should make sure to include
        only valid file types.

        In this case we'll filter on PDFs. Since some sources don't report the PDF mime type correctly
        we'll download the file binary and check that it can be read by a PDF parser.
         */
        LOG.info("Start building the candidate files list.");
        // we'll start with the candidate set of files based on metadata filters.
        List<FileMetadata> fileCandidateList = new ArrayList<>();
        getClient()
                .files()
                .list(Request.create()
                        .withFilterParameter("dataSetIds", List.of(
                                Map.of("externalId", fileSourceDataSetExternalId)
                        ))
                        .withFilterParameter("mimeType", "application/pdf")
                        .withFilterMetadataParameter(fileSourceFilterMetadataKey, fileSourceFilterMetadataValue))
                .forEachRemaining(fileMetadataBatch -> fileCandidateList.addAll(fileMetadataBatch));

        // Then we check if the candidate files are single page PDFs.
        // Since this involves working with file binaries, which are potentially large objects, we'll break everything
        // down into smaller batches so that we don't run out of memory in case we have to evaluate many files.
        List<FileMetadata> inputFiles = new ArrayList<>(); // the set of files satisfying all conditions--our final results
        List<List<FileMetadata>> fileCandidateBatches = Partition.ofSize(fileCandidateList, 5);
        for (List<FileMetadata> fileBatch : fileCandidateBatches) {
            inputFiles.addAll(filterValidPdfs(fileBatch));
        }

        LOG.info("Finished building the candidate files list of {} files. Duration: {}",
                inputFiles.size(),
                Duration.between(startInstant, Instant.now()));

        /*
        Run the files through the interactive P&ID service. In this case we run both detection (generate
        annotations) and convert (generate SVGs).

        It is good practice to iterate through the files in batches so that we don't saturate the api.
         */
        LOG.info("Start detect annotations and convert to SVG.");
        List<List<FileMetadata>> inputFileBatches = Partition.ofSize(inputFiles, 10);
        for (List<FileMetadata> fileBatch : inputFileBatches) {
            // Detect annotations and convert to SVG
            List<PnIDResponse> detectResults = getClient().experimental()
                    .pnid()
                    .detectAnnotationsPnID(mapToItems(fileBatch), matchToEntities, "name", true);

            // Map detect results and input file metadata to a common key and build the
            // SVG file containers that we can write to CDF.
            List<FileContainer> svgContainers = new ArrayList<>();
            Map<Long, PnIDResponse> detectResponseMap = new HashMap<>();
            Map<Long, FileMetadata> fileMetadataMap = new HashMap<>();
            detectResults.stream()
                    .forEach(result -> detectResponseMap.put(result.getFileId().getValue(), result));
            fileBatch.stream()
                    .forEach(fileMetadata -> fileMetadataMap.put(fileMetadata.getId().getValue(), fileMetadata));

            for (Long key : detectResponseMap.keySet()) {
                svgContainers.add(buildSvgFileContainer(detectResponseMap.get(key), fileMetadataMap.get(key)));
            }

            // Write the SVG files to CDF
            getClient().files().upload(svgContainers);
            countFiles += svgContainers.size();
        }

        LOG.info("Finished detect annotations and convert to SVG for {} files. Duration {}",
                countFiles,
                Duration.between(startInstant, Instant.now()));
    }

    /*
    Instantiate the cognite client based on an api key hosted in GCP Secret Manager (key vault).
     */
    private static CogniteClient getClient() throws Exception {
        if (null == client) {
            // Instantiate the client
            LOG.info("Start instantiate the Cognite Client.");

            LOG.info("API key is hosted in Secret Manager.");
            String projectId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[0];
            String secretId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[1];

            client = CogniteClient.ofKey(getGcpSecret(projectId, secretId, "latest"))
                    .withBaseUrl(baseURL);
        }

        return client;
    }

    /*
    Read the data sets. This will be used as a lookup table to map between externalId and internal id
    for data sets.
     */
    private static Map<String, Long> getDataSetExternalIdMap() throws Exception {
        if (null == dataSetExternalIdMap) {
            Map<String, Long> dataSetMap = new HashMap<>();
            getClient()
                    .datasets()
                    .list(Request.create())
                    .forEachRemaining(dataSetBatch ->
                            dataSetBatch.stream()
                                    .forEach(dataSet ->
                                            dataSetMap.put(dataSet.getExternalId().getValue(), dataSet.getId().getValue()))
                    );
        }

        return dataSetExternalIdMap;
    }

    /*
    Read the assets collection and minimize the asset objects.
     */
    private static List<Asset> readAssets() throws Exception {
        List<Asset> assetResults = new ArrayList<>();

        // Read assets based on a list filter. The SDK client gives you an iterator back
        // that lets you "stream" batches of results.
        Iterator<List<Asset>> resultsIterator = getClient().assets().list(Request.create()
                .withFilterParameter("dataSetIds", List.of(
                        Map.of("externalId", assetDataSetExternalId))
                )
                .withFilterMetadataParameter("FACILITY", "ULA")
        );

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

    /*
    Checks if a set of files are valid PDFs and returns the ones that pass the validation.
     */
    private static List<FileMetadata> filterValidPdfs(List<FileMetadata> inputFiles) throws Exception {
        List<FileContainer> fileContainers = getClient()
                .files()
                .download(mapToItems(inputFiles), Paths.get("/temp"), true);

        return fileContainers.stream()
                .filter(fileContainer -> isValidPdf(fileContainer))
                .map(fileContainer -> fileContainer.getFileMetadata())
                .collect(Collectors.toList());
    }

    /*
     * Checks the file based on the following conditions:
     * - The binary is <50MiB
     * - Must be a valid PDF binary.
     * - Must be a single page binary.
     */
    private static boolean isValidPdf(FileContainer fileContainer) {
        if (fileContainer.getFileBinary().getBinary().size() > (1024 * 1024 * 50)) {
            LOG.warn("File binary larger than 50MiB. Name = [{}], id = [{}], externalId = [{}], binary size = [{}]MiB",
                    fileContainer.getFileMetadata().getName().getValue(),
                    fileContainer.getFileMetadata().getId().getValue(),
                    fileContainer.getFileMetadata().getExternalId().getValue(),
                    String.format("%.2f", fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)));

            return false;
        }

        try (PDDocument pdDocument = PDDocument.load(fileContainer.getFileBinary().getBinary().toByteArray())) {
            if (pdDocument.getNumberOfPages() > 1) {
                LOG.warn("File contains more than 1 page. Name = [{}], no pages = [{}], id = [{}], externalId = [{}]",
                        fileContainer.getFileMetadata().getName().getValue(),
                        pdDocument.getNumberOfPages(),
                        fileContainer.getFileMetadata().getId().getValue(),
                        fileContainer.getFileMetadata().getExternalId().getValue());

                return false;
            } else {
                return true;
            }
        } catch (IOException exception) {
            LOG.warn("Error when parsing file: {}. File name = [{}], externalId = [{}], "
                            + "binary length = [{}], binary as string: [{}]",
                    exception.getMessage(),
                    fileContainer.getFileMetadata().getName(),
                    fileContainer.getFileMetadata().getExternalId().getValue(),
                    fileContainer.getFileBinary().getBinary().size(),
                    fileContainer.getFileBinary().getBinary().toStringUtf8().substring(0, 64));

            return false;
        }
    }

    /*
    Maps a set of file (metadata) to items.
     */
    private static List<Item> mapToItems(List<FileMetadata> files) {
        List<Item> resultsList = new ArrayList<>();
        files.stream()
                .map(fileMetadata -> {
                    if (fileMetadata.hasId()) {
                        return Item.newBuilder()
                                .setId(fileMetadata.getId().getValue())
                                .build();
                    } else {
                        return Item.newBuilder()
                                .setExternalId(fileMetadata.getExternalId().getValue())
                                .build();
                    }
                })
                .forEach(item -> resultsList.add(item));

        return resultsList;
    }

    /*
    Builds the file container (file metadata and binary) for an SVG (interactive P&ID) detection result.
    The file binary is collected directly from the detection result and the file metadata is based on the
    origin file's metadata.
     */
    private static FileContainer buildSvgFileContainer(PnIDResponse pnIDResponse, FileMetadata originFileMetadata) throws Exception {
        String originId = originFileMetadata.hasExternalId() ?
                originFileMetadata.getExternalId().getValue() : String.valueOf(originFileMetadata.getId().getValue());
        Long originUploadedTime = originFileMetadata.hasUploadedTime() ?
                originFileMetadata.getUploadedTime().getValue() : -1L;
        String nameNoSuffix =
                originFileMetadata.getName().getValue().replaceFirst("\\.(\\w){1,5}$", "");
        String source = "interactive-pnid-pipeline";
        Long targetDataSetId = getDataSetExternalIdMap().getOrDefault(fileTargetDataSetExternalId, -1L);
        if (targetDataSetId == -1) {
            String message = "Cannot identify id for target dataset: "
                    + fileTargetDataSetExternalId;
            LOG.error(message);
            throw new Exception(message);
        }

        // Build the common file metadata
        ImmutableList skipMetadataKey = ImmutableList.of("content:dos_extension", "content:object_name",
                "content:r_object_id","files:name");
        Map<String, String> metadata = new HashMap<>(originFileMetadata.getMetadataCount());
        originFileMetadata.getMetadataMap().entrySet().stream()
                .filter(entry -> !skipMetadataKey.contains(entry.getKey()))
                .forEach(entry -> metadata.put(entry.getKey(), entry.getValue()));

        FileMetadata.Builder commonMetadataBuilder = FileMetadata.newBuilder()
                .setDataSetId(Int64Value.of(targetDataSetId))
                .addAllAssetIds(originFileMetadata.getAssetIdsList())
                .putAllMetadata(metadata)
                .putMetadata(originDocExtIdKey, originFileMetadata.getExternalId().getValue())
                .putMetadata(originDocUploadedTimeKey, String.valueOf(originUploadedTime))
                .putMetadata(contextAgentKey, contextAgent)
                .putMetadata(contextAlgorithmKey, contextAlgorithm);

        // Must check if the source document has these parameters. If not, they will incorrectly be set to "empty".
        if (originFileMetadata.hasDirectory()) {
            commonMetadataBuilder.setDirectory(originFileMetadata.getDirectory());
        }
        if (originFileMetadata.hasSource()) {
            commonMetadataBuilder.setSource(originFileMetadata.getSource());
        }
        FileMetadata commonMetadata = commonMetadataBuilder.build();

        // Build the file container
        FileContainer.Builder svgContainerBuilder = FileContainer.newBuilder()
                .setFileMetadata(FileMetadata.newBuilder(commonMetadata)
                        .setExternalId(StringValue.of("convSvg:" + originId))
                        .setName(StringValue.of(nameNoSuffix + ".svg"))
                        .setMimeType(StringValue.of("image/svg+xml")));

        if (pnIDResponse.hasSvgBinary()) {
            svgContainerBuilder.setFileBinary(FileBinary.newBuilder()
                    .setBinary(pnIDResponse.getSvgBinary().getValue()));
        }

        return svgContainerBuilder.build();
    }

    /*
    Read secrets from GCP Secret Manager.
    If we are using workload identity on GKE, we have to take into account that the identity metadata
    service for the pod may take a few seconds to initialize. Therefore the implicit call to get
    identity may fail if it happens at the very start of the pod. The workaround is to perform a
    retry.
     */
    private static String getGcpSecret(String projectId, String secretId, String secretVersion) throws IOException {
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
