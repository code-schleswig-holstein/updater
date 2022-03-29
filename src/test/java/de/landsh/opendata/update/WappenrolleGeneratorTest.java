package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dom4j.io.SAXReader;
import org.json.JSONArray;
import org.json.JSONTokener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.unlimited;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class WappenrolleGeneratorTest {
    private static final Logger log = LoggerFactory.getLogger(WappenrolleGeneratorTest.class);
    private static ClientAndServer mockServer;

    @BeforeAll
    public static void startServer() throws IOException, InterruptedException {
        mockServer = startClientAndServer(1080);

        log.info("mockserver.isRunning: " + mockServer.isRunning());
        log.info("mockserver.hasStarted: " + mockServer.hasStarted());

        while (!mockServer.isRunning()) {
            Thread.sleep(100);
        }

        byte[] rawdata = IOUtils.toByteArray(WappenrolleGeneratorTest.class.getResourceAsStream("/wr_opendata.xml"));

        new MockServerClient("127.0.0.1", mockServer.getPort())
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/wr_opendata.xml"),
                        unlimited())
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "text/xml"))
                                .withBody(rawdata)
                );

    }

    @AfterAll
    public static void stopServer() {
        mockServer.stop(true);
        while (!mockServer.hasStopped(3, 100L, TimeUnit.MILLISECONDS)) {
        }
    }

    @Test
    public void convertDate() {
        assertNull(WappenrolleGenerator.convertDate(null));
        assertNull(WappenrolleGenerator.convertDate(""));
        assertNull(WappenrolleGenerator.convertDate("   "));
        assertEquals("2020-05-08", WappenrolleGenerator.convertDate("08.05.2020"));
    }

    @Test
    public void wappen() throws Exception {
        File dir = Files.createTempDirectory("tmp").toFile();

        DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.getGeneratorArgs().put("type", "wappen");
        update.setOriginalURL("http://localhost:" + mockServer.getPort() + "/wr_opendata.xml");

        final WappenrolleGenerator wr = new WappenrolleGenerator(update);
        wr.generateDistributions(dir);


        // Parse JSON
        JSONArray json = new JSONArray(new JSONTokener(new FileReader(new File(dir, "wappen.json"))));
        assertTrue(json.length() > 0);

        // Parse XML
        final SAXReader reader = new SAXReader();
        reader.read(new File(dir, "wappen.rdf"));

        // clean up
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void flaggen() throws Exception {
        File dir = Files.createTempDirectory("tmp").toFile();

        DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.getGeneratorArgs().put("type", "flaggen");
        update.setOriginalURL("http://localhost:" + mockServer.getPort() + "/wr_opendata.xml");

        final WappenrolleGenerator wr = new WappenrolleGenerator(update);
        wr.generateDistributions(dir);

        // Parse JSON
        final JSONArray json = new JSONArray(new JSONTokener(new FileReader(new File(dir, "flaggen.json"))));
        assertTrue(json.length() > 0);


        // Parse XML
        final SAXReader reader = new SAXReader();
        reader.read(new File(dir, "flaggen.rdf"));

        // clean up
        FileUtils.deleteDirectory(dir);
    }
}
