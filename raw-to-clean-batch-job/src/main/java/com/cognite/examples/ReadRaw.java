package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.dto.RawRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class ReadRaw {
    private static Logger LOG = LoggerFactory.getLogger(ReadRaw.class);
    public static void main(String[] args) throws Exception {
        Instant startInstant = Instant.now();
        String dbName = "PIAF_test";
        String dbTable = "attributes_prod";
        List<String> columns = Arrays.asList("Name", "Description", "Path", "DataReferencePlugIn",
                "ConfigString", "ParentElementWebId");
        Predicate<RawRow> isBitValue =
                schema -> schema.getColumns().getFieldsMap().get("DataReferencePlugIn").getStringValue().equals("BitValue");

        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);

        CogniteClient client = CogniteClient.ofKey(System.getenv("AKERBP_TEST_KEY"));

        List<RawRow> listRowsResults = new ArrayList<>();
        client.raw().rows().list(dbName, dbTable, columns)
                //.forEachRemaining(results -> results.clear())
                .forEachRemaining(results -> results.stream()
                        .filter(isBitValue)
                        .forEach(row -> listRowsResults.add(row.toBuilder()
                                .clearDbName()
                                .clearTableName()
                                .build())))
        ;

        LOG.info("Finished reading {} rows from raw. Duration {}",
                listRowsResults.size(),
                Duration.between(startInstant, Instant.now()));
    }
}
