package com.cognite.sa.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Class that defines the HTTP endpoints for the copy table resource / functionality.
 */
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("copy")
public class CopyTableResource {
    private static final Logger LOG = LoggerFactory.getLogger(CopyTableResource.class);

    @Inject
    CopyTableService copyService;

    @POST
    public CopyTableResponse postCopy(CopyTableRequest copyRequest) {
        if (copyRequest == null) {
            throw new BadRequestException();
        }
        LOG.info("Received request: {}", copyRequest);
        if (copyRequest.sourceDb == null || copyRequest.sourceTable == null || copyRequest.targetDb == null
                || copyRequest.targetTable == null) {
            String message = "Request does not contain the required fields.";
            LOG.warn(message);
            throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(message)).type(MediaType.APPLICATION_JSON)
                    .build());
        }

        try {
            return copyService.copyTable(copyRequest);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage())).type(MediaType.APPLICATION_JSON)
                    .build());
        }
    }

    @GET
    public CopyTableResponse getCopy() {
        CopyTableResponse response = new CopyTableResponse();
        response.message = "The service is alive--but no copy operation has been performed.";
        return response;
    }
}
