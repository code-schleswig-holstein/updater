package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpStatusCode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.unlimited;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

class DenkmallisteGeneratorTest {
    private static ClientAndServer mockServer;

    @BeforeAll
    public static void startServer() throws IOException {
        mockServer = startClientAndServer(1080);

        byte[] rawdata = IOUtils.toByteArray(DenkmallisteGeneratorTest.class.getResourceAsStream("/denkmalliste1.json"));
        byte[] pdfData = new byte[100];

        final MockServerClient mockServerClient = new  MockServerClient("127.0.0.1", mockServer.getPort());
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/data.json"),
                        unlimited())
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "application/json"))
                                .withBody(rawdata)
                );
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/list.pdf"),
                        unlimited())
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "application/pdf"))
                                .withBody(pdfData)
                );
    }

    @AfterAll
    public static void stopServer() throws IOException {
        mockServer.stop();
    }

    @Test
    void generateDistributions_complete() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.getGeneratorArgs().put("json", "http://localhost:" + mockServer.getPort() + "/data.json");
        update.getGeneratorArgs().put("photoDirectory","/tmp/");
        update.getGeneratorArgs().put("photoBaseURL","https://example.org/photo/");

        final File dir = Files.createTempDirectory("tmp").toFile();

        final DenkmallisteGenerator generator = new DenkmallisteGenerator(update);
        generator.generateDistributions(dir);

        final File[] resultFiles =   dir.listFiles();
        assertNotNull(resultFiles);
        assertEquals(3, resultFiles.length);
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "denkmalliste.json".equals( f.getName())));
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "denkmalliste.csv".equals( f.getName())));
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "denkmalliste-latin1.csv".equals( f.getName())));

        FileUtils.deleteDirectory(dir);
    }

    @Test
    void generateDistributions_oneCounty() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.getGeneratorArgs().put("json", "http://localhost:" + mockServer.getPort() + "/data.json");
        update.getGeneratorArgs().put("photoDirectory","/tmp/");
        update.getGeneratorArgs().put("photoBaseURL","https://example.org/photo/");
        update.getGeneratorArgs().put("county","Stadt Flensburg");
        update.getGeneratorArgs().put("pdf","http://localhost:" + mockServer.getPort() + "/list.pdf");

        final File dir = Files.createTempDirectory("tmp").toFile();

        final DenkmallisteGenerator generator = new DenkmallisteGenerator(update);
        generator.generateDistributions(dir);

        final File[] resultFiles =   dir.listFiles();
        assertNotNull(resultFiles);
        assertEquals(4, resultFiles.length);
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "Stadt Flensburg.pdf".equals( f.getName())));
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "Stadt Flensburg.json".equals( f.getName())));
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "Stadt Flensburg.csv".equals( f.getName())));
        assertTrue(Arrays.stream(resultFiles).anyMatch(f -> "Stadt Flensburg-latin1.csv".equals( f.getName())));

        FileUtils.deleteDirectory(dir);
    }

}