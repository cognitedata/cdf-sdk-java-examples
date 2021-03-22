package com.cognite.sa.api;

import com.cognite.client.CogniteClient;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

/**
 * This class hosts shared resources for all services.
 *
 * The CogniteClient should be used as a single instance across all operations (towards a given
 * CDF project / tenant) in an application. By hosting the client as a singleton in this central
 * bean, we make sure that we don't create too many instances of the client.
 */
@ApplicationScoped
public class SharedResources {
    private final static Logger LOG = LoggerFactory.getLogger(SharedResources.class);
    private final static String UNSET = "unset";

    @ConfigProperty(name = "cdf.key.env")
    String apiKey;

    @ConfigProperty(name = "cdf.key.secret-manager")
    String secretUri;

    @ConfigProperty(name = "cdf.base-url")
    String baseURL;

    private CogniteClient client;

    //@ApplicationScoped
    //@Produces
    synchronized CogniteClient getClient() throws Exception {
        if (client == null) {
            // Instantiate the client
            LOG.info("Start instantiate the Cognite Client.");
            if (!apiKey.equals(UNSET)) {
                LOG.info("API key read from env variable.");
                client = CogniteClient.ofKey(apiKey)
                        .withBaseUrl(baseURL);
            } else if (!secretUri.equals(UNSET)) {
                LOG.info("API key is hosted in Secret Manager.");
                String projectId = secretUri.split("\\.")[0];
                String secretId = secretUri.split("\\.")[1];
                client = CogniteClient.ofKey(getGcpSecret(projectId, secretId, "latest"))
                        .withBaseUrl(baseURL);
            } else {
                String message = "API key is not configured.";
                LOG.error(message);
                throw new Exception(message);
            }
        }

        return client;
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
