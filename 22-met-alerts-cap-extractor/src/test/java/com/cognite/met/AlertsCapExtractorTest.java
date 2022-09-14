package com.cognite.met;

import com.cognite.client.dto.RawRow;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class AlertsCapExtractorTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void parseCapXml() throws Exception {
        Path capXmlFile = Paths.get("./src/test/resources/cap.xml");
        String capXmlString = Files.readString(capXmlFile);
        RawRow capRow = AlertsCapExtractor.parseRawRow(capXmlString);
        LOG.info("Cap Row: /n{}", capRow);

    }

}