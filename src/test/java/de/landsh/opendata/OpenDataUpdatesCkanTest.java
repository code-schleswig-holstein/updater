package de.landsh.opendata;

import de.landsh.opendata.ckan.ApiKey;
import de.landsh.opendata.ckan.CkanAPI;
import de.landsh.opendata.ckan.Resource;
import de.landsh.opendata.update.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpStatusCode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static de.landsh.opendata.OpenDataUpdatesCkan.getExtrasValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class OpenDataUpdatesCkanTest {

    static final String TODAY = LocalDate.now().format(DateTimeFormatter.ISO_DATE) + "T";
    private static ClientAndServer mockServer;
    private final OpenDataUpdatesCkan openDataUpdatesCkan = new OpenDataUpdatesCkan("http://localhost:8080", new ApiKey(""));
    private final CkanAPI ckanAPI = Mockito.mock(CkanAPI.class);
    private final CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
    private File localDataDir;

    @BeforeAll
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterAll
    public static void stopServer() {
        mockServer.stop(true);
        while (!mockServer.hasStopped(3, 100L, TimeUnit.MILLISECONDS)) {
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        openDataUpdatesCkan.setCkanAPI(ckanAPI);
        localDataDir = Files.createTempDirectory("localdata").toFile();
        openDataUpdatesCkan.setLocalDataDir(localDataDir);
        DummyGenerator.invokationCounter = 0;
    }

    @Test
    public void work_unchanged() throws Exception {
        byte[] DATA = "Hello, world!".getBytes();
        final DatasetUpdate update = new DatasetUpdate();

        update.datasetId = "my-dataset1";
        update.originalURL = "http://localhost:" + mockServer.getPort() + "/data.csv";
        update.generator = "just-download";

        JSONTokener jsonTokener = new JSONTokener(getClass().getResourceAsStream("/package_show__badegewasser-stammdaten1.json"));
        JSONObject dataset = new JSONObject(jsonTokener);
        Mockito.when(ckanAPI.readDataset("my-dataset1")).thenReturn(dataset);

        Resource resource = new Resource();
        resource.setAccessURL(update.originalURL);
        Mockito.when(ckanAPI.getResource(dataset, true)).thenReturn(resource);

        new MockServerClient("127.0.0.1", mockServer.getPort())
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/data.csv"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "text/plain; charset=utf-8"))
                                .withBody(DATA)
                );

        File dataDir = new File(localDataDir, "my-dataset1");
        dataDir.mkdir();

        FileOutputStream localCopy = new FileOutputStream(new File(dataDir, "my-dataset1_2020-07-09T10:26:43.17"));
        localCopy.write(DATA);
        localCopy.close();

        boolean result = openDataUpdatesCkan.work(update);

        Mockito.verify(ckanAPI, times(0)).updatePackage(ArgumentMatchers.any());

        assertTrue(result);
    }

    @Test
    public void work_append() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();

        update.datasetId = "my-dataset1";
        update.originalURL = "http://localhost:" + mockServer.getPort() + "/data.csv";
        update.type = DatasetUpdate.Type.APPEND;
        update.generator = "just-download";

        JSONTokener jsonTokener = new JSONTokener(getClass().getResourceAsStream("/package_show__badegewasser-stammdaten1.json"));
        JSONObject dataset = new JSONObject(jsonTokener);
        Mockito.when(ckanAPI.readDataset("my-dataset1")).thenReturn(dataset);

        Resource resource = new Resource();
        resource.setAccessURL(update.originalURL);
        Mockito.when(ckanAPI.getResource(dataset, true)).thenReturn(resource);

        new MockServerClient("127.0.0.1", mockServer.getPort())
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/data.csv"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "text/plain; charset=utf-8"))
                                .withBody("Hello, world!")
                );

        final File dataDir = new File(localDataDir, update.datasetId);
        dataDir.mkdir();

        final FileOutputStream localCopy = new FileOutputStream(new File(localDataDir, "my-dataset1_2020-07-09T10:26:43.17"));
        localCopy.write("Hello, world!\nHello, moon!".getBytes());
        localCopy.close();

        final ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);
        Mockito.when(ckanAPI.updatePackage(argument.capture())).thenReturn(true);

        LocalDateTime timeBefore = LocalDateTime.now();

        boolean result = openDataUpdatesCkan.work(update);

        LocalDateTime timeAfter = LocalDateTime.now();

        assertTrue(result);
        Mockito.verify(ckanAPI, times(1)).updatePackage(ArgumentMatchers.any());
        JSONObject modifiedDataset = argument.getValue();
        LocalDateTime modified = LocalDateTime.parse(getExtrasValue(modifiedDataset, "modified"));
        assertTrue(modified.isAfter(timeBefore));
        assertTrue(modified.isBefore(timeAfter));

        assertNull(getExtrasValue(modifiedDataset, "temporal_end"));
    }

    /**
     * The program must not terminate if a generator fails to work.
     */
    @Test
    public void work_generatorFails() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.datasetId = "my-dataset1";
        update.generator = DenkmallisteGenerator.class.getCanonicalName();
        update.originalURL = "http://localhost:" + mockServer.getPort() + "/data.csv";
        update.generatorArgs = new HashMap<>();
        update.generatorArgs.put("json", "http://localhost:" + mockServer.getPort() + "/data.json");

        JSONTokener jsonTokener = new JSONTokener(getClass().getResourceAsStream("/package_show__badegewasser-stammdaten1.json"));
        JSONObject dataset = new JSONObject(jsonTokener);
        Mockito.when(ckanAPI.readDataset("my-dataset1")).thenReturn(dataset);

        Resource resource = new Resource();
        resource.setAccessURL(update.originalURL);
        Mockito.when(ckanAPI.getResource(dataset, true)).thenReturn(resource);

        new MockServerClient("127.0.0.1", mockServer.getPort())
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/data.json"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.NOT_FOUND_404.code())
                );

        final boolean result = openDataUpdatesCkan.work(update);

        assertFalse(result);
    }

    /**
     * Erzeugt das Verzeichnis und die Datei mit den angeblich zuletzt gespeicherten Daten
     *
     * @return die erzeugte Datei mit den  angeblich zuletzt gespeicherten Daten
     */
    private File createFakeLocalCopy(String id, String fileSuffix, String content) throws IOException {
        final File dataDir = new File(localDataDir, id);
        dataDir.mkdir();
        final File dataFile = new File(dataDir, id + "." + fileSuffix);
        final FileWriter writer = new FileWriter(dataFile);
        writer.write(content);
        writer.close();
        return dataFile;
    }

    @Test
    public void work_overwritePublic() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.collectionId = "standorte-sh_wlan";
        update.originalURL = "http://localhost:" + mockServer.getPort() + "/aps.geojson";
        update.generator = JustDownloadGenerator.class.getCanonicalName();
        update.format = "json";
        update.type = DatasetUpdate.Type.OVERWRITE;

        // prepare the existing local copy file
        final String DATA = "{\"type\": \"FeatureCollection\", \"features\": [{\"type\": \"Feature\", \"properties\": {\"name\": \"Omnis-R310-020\"}, \"geometry\": {\"type\": \"Point\", \"coordinates\": [10.49293, 54.386352, 0.0]}}]}";
        final String DATA_NEW = "{\"type\": \"FeatureCollection\", \"features\": [{\"type\": \"Feature\", \"properties\": { \"geometry\": {\"type\": \"Point\", \"coordinates\": [10.49293, 54.386352, 0.0]}}]}";
        createFakeLocalCopy("standorte-sh_wlan", "json", DATA);

        new MockServerClient("127.0.0.1", mockServer.getPort())
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/aps.geojson"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(HttpStatusCode.OK_200.code())
                                .withHeaders(
                                        new Header("Content-Type", "application/json; charset=utf-8"))
                                .withBody(DATA_NEW)
                );

        Mockito.when(ckanAPI.findNewestDataset("standorte-sh_wlan")).thenReturn("standorte-sh_wlan-aktuell");

        final JSONTokener jsonTokener = new JSONTokener(getClass().getResourceAsStream("/package_show__standorte-sh_wlan-aktuell.json"));
        final JSONObject dataset = new JSONObject(jsonTokener);
        Mockito.when(ckanAPI.readDataset("standorte-sh_wlan-aktuell")).thenReturn(dataset);

        final String newId = UUID.randomUUID().toString();
        final ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);
        Mockito.when(ckanAPI.createPackage(argument.capture())).thenReturn(newId);

        Mockito.when(ckanAPI.changeTitle(newId, "Standorte #SH_WLAN")).thenReturn(true);

        final ArgumentCaptor<File> argumentFile = ArgumentCaptor.forClass(File.class);
        Mockito.when(ckanAPI.uploadFile(eq(newId), argumentFile.capture(), eq("standorte-sh_wlan.json"),
                eq("JSON"), eq("application/json"))).thenReturn(true);

        Mockito.when(ckanAPI.putDatasetInCollection(newId, "standorte-sh_wlan")).thenReturn(true);

        final ArgumentCaptor<JSONObject> argumentExistingDataset = ArgumentCaptor.forClass(JSONObject.class);
        Mockito.when(ckanAPI.updatePackage(argumentExistingDataset.capture())).thenReturn(true);

        boolean result = openDataUpdatesCkan.work(update);
        assertTrue(result);
        Mockito.verify(ckanAPI, times(1)).putDatasetInCollection(newId, "standorte-sh_wlan");

        JSONObject newDataset = argument.getValue();
        JSONObject existingDataset = argumentExistingDataset.getValue();
        File submittedFile = argumentFile.getValue();
        // TODO check the content of the submitted file

        assertTrue(getExtrasValue(newDataset, "modified").startsWith(TODAY));
        assertTrue(getExtrasValue(newDataset, "issued").startsWith(TODAY));
        assertEquals("2021-11-03T00:00:00", getExtrasValue(newDataset, "temporal_start"));
        assertTrue(getExtrasValue(newDataset, "temporal_end").startsWith(TODAY));

        assertTrue(getExtrasValue(existingDataset, "modified").startsWith(TODAY));
        assertTrue(getExtrasValue(existingDataset, "issued").startsWith(TODAY));
        assertTrue(getExtrasValue(existingDataset, "temporal_start").startsWith(TODAY));
        assertEquals("", getExtrasValue(existingDataset, "temporal_end"));
    }


    @Test
    public void findGeneratorDynamically_NoSuchClass() throws Exception {
        Generator result = openDataUpdatesCkan.findGeneratorDynamically(
                "de.landsh.opendata.update.FooBar",
                "test",
                new DatasetUpdate());
        assertNull(result);
    }

    @Test
    public void findGeneratorDynamically_NoParameters() throws Exception {
        Generator result = openDataUpdatesCkan.findGeneratorDynamically(
                NoopGenerator.class.getCanonicalName(),
                "test",
                new DatasetUpdate()
        );
        assertNotNull(result);
    }

    @Test
    public void findGeneratorDynamically_AllParameters() throws Exception {
        Generator result = openDataUpdatesCkan.findGeneratorDynamically(
                JustDownloadGenerator.class.getCanonicalName(),
                "test",
                new DatasetUpdate());
        assertNotNull(result);
    }

    @Test
    public void work_missingDirectory() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.collectionId = "dummy";
        update.generator = DummyGenerator.class.getCanonicalName();
        update.type = DatasetUpdate.Type.APPEND;

        boolean result = openDataUpdatesCkan.work(update);
        assertFalse(result);
    }

    @Test
    public void work_appendPrivate_missingDatasetId() throws Exception {
        final DatasetUpdate update = new DatasetUpdate();
        update.collectionId = "dummy";
        update.generator = WriteNewFileGenerator.class.getCanonicalName();
        update.format = "csv";
        update.type = DatasetUpdate.Type.APPEND;
        update.isPrivate = true;

        createFakeLocalCopy("dummy", "csv", "old data");

        boolean result = openDataUpdatesCkan.work(update);
        assertFalse(result);
    }


    @Test
    public void work_appendPrivate() throws Exception {
        final String id = "testungen-in-der-schule-mit-einem-positiven-testergebnis";
        final DatasetUpdate update = new DatasetUpdate();
        update.datasetId = id;
        update.generator = WriteNewFileGenerator.class.getCanonicalName();
        update.format = "csv";
        update.type = DatasetUpdate.Type.APPEND;
        update.isPrivate = true;

        // prepare files
        createFakeLocalCopy(id, "csv", "old data");

        // mock CKAN API
        final JSONTokener jsonTokener = new JSONTokener(getClass().getResourceAsStream("/package_show__testungen-in-der-schule-mit-einem-positiven-testergebnis.json"));
        final JSONObject dataset = new JSONObject(jsonTokener);
        Mockito.when(ckanAPI.readDataset(id)).thenReturn(dataset);
        Mockito.when(ckanAPI.getResources(any())).thenCallRealMethod();


        // invoke method
        boolean result = openDataUpdatesCkan.work(update);

        // check results
        assertTrue(result);

        Mockito.verify(ckanAPI, times(1)).deleteResource("9de8c47d-ada0-4446-bd28-b9ab1f5396f9");
        final ArgumentCaptor<JSONObject> argument = ArgumentCaptor.forClass(JSONObject.class);
        Mockito.verify(ckanAPI, times(1)).updatePackage(argument.capture());
        JSONObject updatedDataset = argument.getValue();
        assertNotNull(updatedDataset);

        Mockito.verify(ckanAPI, times(1))
                .uploadFile(eq("testungen-in-der-schule-mit-einem-positiven-testergebnis"),
                        ArgumentMatchers.any(File.class),
                        eq("testungen-in-der-schule-mit-einem-positiven-testergebnis.csv"),
                        eq("CSV"),
                        eq("text/csv"));

    }

    /**
     * Der Datensatz soll nur maximal jeden Monat aktualisiert werden.
     */
    @Test
    void work_noUpdateNecessary() throws Exception {
        final String datasetId = "5Qknzt7iPCLUc";

        createFakeLocalCopy(datasetId, "csv", "old data");

        final DatasetUpdate update = new DatasetUpdate();
        update.setType(DatasetUpdate.Type.APPEND);
        update.setFrequency(DatasetUpdate.Frequency.MONTHLY);
        update.setDatasetId(datasetId);
        update.setGenerator(DummyGenerator.class.getName());

        openDataUpdatesCkan.work(update);

        assertEquals(0, DummyGenerator.invokationCounter);
    }

    /**
     * Der Datensatz soll nur maximal jede Woche aktualisiert werden, die Zeit ist bereits erreicht.
     */
    @Test
    void work_updateNecessary() throws Exception {
        final String datasetId = "ed1caML57fUtg";

        // erzeuge eine alte lokale "Kopie" der Daten
        final File oldFile = createFakeLocalCopy(datasetId, "csv", "old data");
        final LocalDateTime newLocalDateTime = LocalDateTime.now();
        final Instant instant = newLocalDateTime.minusDays(8).toInstant(ZoneOffset.UTC);
        Files.setLastModifiedTime(oldFile.toPath(), FileTime.from(instant));

        final DatasetUpdate update = new DatasetUpdate();
        update.setType(DatasetUpdate.Type.APPEND);
        update.setFrequency(DatasetUpdate.Frequency.WEEKLY);
        update.setDatasetId(datasetId);
        update.setGenerator(DummyGenerator.class.getName());

        openDataUpdatesCkan.work(update);

        assertEquals(1, DummyGenerator.invokationCounter);
    }


}
