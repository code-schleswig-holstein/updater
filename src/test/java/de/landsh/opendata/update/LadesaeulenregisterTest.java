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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.unlimited;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

class LadesaeulenregisterTest {
    private static final Logger log = LoggerFactory.getLogger(LadesaeulenregisterTest.class);
    private static ClientAndServer mockServer;

    @BeforeAll
    public static void startServer() throws IOException, InterruptedException {
        mockServer = startClientAndServer(1080);

        log.info("mockserver.isRunning: " + mockServer.isRunning());
        log.info("mockserver.hasStarted: " + mockServer.hasStarted());

        while (!mockServer.isRunning()) {
            Thread.sleep(100);
        }

        byte[] excelFile = IOUtils.toByteArray(LadesaeulenregisterTest.class.getResourceAsStream("/ladesaeulenregister/Ladesaeulenregister.xlsx"));
        byte[] htmlFile = IOUtils.toByteArray(LadesaeulenregisterTest.class.getResourceAsStream("/ladesaeulenregister/start.html"));

        final MockServerClient mockServerClient = new MockServerClient("127.0.0.1", mockServer.getPort());
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx;jsessionid=339140FDA38212574F7EF35C74B35447"),
                        unlimited())
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                                .withBody(excelFile)
                );
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html"),
                        unlimited())
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "text/html"))
                                .withBody(htmlFile)
                );
    }

    @AfterAll
    public static void stopServer() {
        mockServer.stop();
    }

    @Test
    public void determineExcelFileURL() throws IOException {
        final String expectedURL = "http://localhost:" + mockServer.getPort() + "/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx;jsessionid=339140FDA38212574F7EF35C74B35447?__blob=publicationFile&amp;v=23";

        DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.setOriginalURL("http://localhost:" + mockServer.getPort() + "/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html");

        final Ladesaeulenregister generator = new Ladesaeulenregister(update);

        String url = generator.determineExcelFileURL();

        assertEquals(expectedURL, url);
    }

    @Test
    public void generate() throws Exception {
        File dir = Files.createTempDirectory("tmp").toFile();

        DatasetUpdate update = new DatasetUpdate();
        update.setGeneratorArgs(new HashMap<>());
        update.setOriginalURL("http://localhost:" + mockServer.getPort() + "/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html");

        final Ladesaeulenregister generator = new Ladesaeulenregister(update);
        boolean result = generator.generateDistributions(dir);

        assertTrue(result);

        final File csvFile = new File(dir, "ladesaeulenregister.csv");
        assertTrue(csvFile.exists());

        final List<String> lines = Files.readAllLines(csvFile.toPath());
        assertTrue(lines.get(0).startsWith("Betreiber,Straße,"));
        assertTrue(lines.get(1).startsWith("EnBW mobility+ AG"));
        assertEquals(23, lines.size());

        // clean up
        FileUtils.deleteDirectory(dir);
    }
}
