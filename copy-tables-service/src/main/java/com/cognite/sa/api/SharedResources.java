package com.cognite.sa.api;

import com.cognite.client.CogniteClient;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Produces;

/**
 * This class hosts shared resources for all services.
 *
 * The CogniteClient should be used as a single instance across all operations (towards a given
 * CDF project / tenant) in an application. By hosting the client as a singleton in this central
 * bean, we make sure that we don't create too many instances of the client.
 */
@ApplicationScoped
public class SharedResources {

    private CogniteClient client;

    @Produces
    public CogniteClient getClient() {
        if (client == null) {
            // Instantiate the client

        }

        return client;
    }
}
