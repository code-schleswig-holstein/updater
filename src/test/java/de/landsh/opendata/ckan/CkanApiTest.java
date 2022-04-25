package de.landsh.opendata.ckan;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHeader;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class CkanApiTest {

    private final ApiKey apiKey = new ApiKey("dummy");
    private final RestClient restClient = Mockito.mock(RestClient.class);
    private CkanAPI ckanAPI;

    private JSONObject datasetNotFound;

    private static JSONObject jsonFromResource(String resourceName) throws IOException {
        return new JSONObject(IOUtils.toString(Objects.requireNonNull(CkanApiTest.class.getResourceAsStream(resourceName)), StandardCharsets.UTF_8));

    }

    @BeforeEach
    public void setUp() {
        ckanAPI = new CkanAPI("http://localhost", apiKey);
        ckanAPI.setRestClient(restClient);

        datasetNotFound = new JSONObject();
        datasetNotFound.put("success", false);
        datasetNotFound.put("error", new JSONObject());
        datasetNotFound.getJSONObject("error").put("message", "Nicht gefunden");
    }

    @Test
    public void readDataset() throws Exception {
        JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        final JSONObject dataset = ckanAPI.readDataset("kindertagesstatten1");
        assertNotNull(dataset);

        assertEquals(new URI("http://localhost/api/3/action/package_show?id=kindertagesstatten1"), argument.getValue().getURI());
    }

    @Test
    public void getCollection() throws Exception {
        JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        String result = ckanAPI.getCollection("kindertagesstatten1");
        assertEquals("ed667223-6205-43f6-a2da-0acba4d53ddd", result);
    }

    @Test
    public void getCollection2() throws Exception {
        final JSONObject json = jsonFromResource("/package_show__dataset_in_collection.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        String result = ckanAPI.getCollection("badegewasser-infrastruktur1");
        assertEquals("6f30a595-9210-4f24-8873-b52c72401468", result);
    }

    /**
     * It is possible the the "object_package_id" field is null.
     */
    @Test
    public void getCollectionNull() throws Exception {
        final JSONObject json = jsonFromResource("/package_show__relationship_null.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        assertNull(ckanAPI.getCollection("geschaftsverteilungsplan-melund-stand-15-07-2020"));
    }

    @Test
    public void getOrganization() throws Exception {
        final JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        String result = ckanAPI.getOrganization("kindertagesstatten1");
        assertEquals("f2d024c8-dbcc-4786-837e-d4eca1a23a57", result);
    }

    @Test
    public void getAccessURL() throws IOException {
        final JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json);

        String result = ckanAPI.getAccessURL("kindertagesstatten1");
        assertEquals("http://185.223.104.6/data/sozmin/kita_2019-09-18.csv", result);
    }

    @Test
    public void findNewestDataset() throws Exception {
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(mockResponse.getFirstHeader("Location")).thenReturn(new BasicHeader("Location", "https://opendata.sh/dataset/mydata"));

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeRawHttpRequest(argument.capture())).thenReturn(mockResponse);

        String result = ckanAPI.findNewestDataset("mycollection");
        assertEquals("mydata", result);
        assertEquals(new URI("http://localhost/collection/mycollection/aktuell"), argument.getValue().getURI());
    }

    @Test
    public void getResource() throws Exception {
        final JSONObject dataset = jsonFromResource("/package_show__kindertagesstatten1.json");

        final Resource result = ckanAPI.getResource(dataset, true);
        assertNotNull(result);
        assertEquals("kita.csv", result.getName());
        assertEquals("96948a3b-b1ca-407c-a33a-60a9ebc49c78", result.getId());
        assertEquals("CSV", result.getFormat());
        assertEquals("http://185.223.104.6/data/sozmin/kita_2019-09-18.csv", result.getAccessURL());
        assertEquals(300618, result.getByteSize());
        assertEquals("text/csv", result.getMimeType());
        assertNull(result.getChecksum());
    }

    @Test
    public void getResource_2() throws IOException {
        final JSONObject dataset = jsonFromResource("/package_show__badegewasser-stammdaten1.json");
        final Resource result = ckanAPI.getResource(dataset, true);
        assertNotNull(result);
    }

    @Test
    public void getResource_noResource() throws Exception {
        final JSONObject dataset = jsonFromResource("/package_show__kindertagesstaetten.json");
        final Resource result = ckanAPI.getResource(dataset, true);

        assertNull(result);
    }

    @Test
    public void putDatasetInCollection_missingCollection() throws IOException, URISyntaxException {
        JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json, datasetNotFound);

        try {
            ckanAPI.putDatasetInCollection("mydataset", "mycollection");
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("mycollection"));
        }

        assertEquals(new URI("http://localhost/api/3/action/package_show?id=mydataset"), argument.getAllValues().get(0).getURI());
        assertEquals(new URI("http://localhost/api/3/action/package_show?id=mycollection"), argument.getAllValues().get(1).getURI());
    }

    @Test
    public void putDatasetInCollection_missingDataset() throws IOException, URISyntaxException {
        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(datasetNotFound);

        try {
            ckanAPI.putDatasetInCollection("mydataset", "mycollection");
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("mydataset"));
        }

        assertEquals(new URI("http://localhost/api/3/action/package_show?id=mydataset"), argument.getAllValues().get(0).getURI());
    }

    /**
     * The specified dataset id does not belong to a collection.
     */
    @Test
    public void putDatasetInCollection_noCollection() throws IOException, URISyntaxException {
        JSONObject json = jsonFromResource("/package_show__kindertagesstatten1.json");

        final ArgumentCaptor<HttpUriRequest> argument = ArgumentCaptor.forClass(HttpUriRequest.class);
        Mockito.when(restClient.executeHttpRequest(argument.capture())).thenReturn(json, json);

        try {
            ckanAPI.putDatasetInCollection("mydataset", "mycollection");
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("mycollection"));
            assertTrue(expected.getMessage().contains("is no collection"));
        }

        assertEquals(new URI("http://localhost/api/3/action/package_show?id=mydataset"), argument.getAllValues().get(0).getURI());
        assertEquals(new URI("http://localhost/api/3/action/package_show?id=mycollection"), argument.getAllValues().get(1).getURI());
    }

    @Test
    public void getResources() throws IOException {
        final JSONObject json = jsonFromResource("/package_show__testungen-in-der-schule-mit-einem-positiven-testergebnis.json");
        // invoke method
        final List<Resource> result = ckanAPI.getResources(json);

        // check results
        assertNotNull(result);
        assertEquals(1, result.size());
        Resource resource = result.get(0);
        assertEquals("9de8c47d-ada0-4446-bd28-b9ab1f5396f9", resource.getId());
        assertEquals("https://opendata-stage.schleswig-holstein.de/dataset/f17419f7-d705-487b-9a96-00778c53ea65/resource/9de8c47d-ada0-4446-bd28-b9ab1f5396f9/download/data.csv", resource.getAccessURL());
        assertEquals("6bea861d37b495bbb9ef7ad401ee6291", resource.getChecksum());
        assertEquals("CSV", resource.getFormat());
        assertEquals("data.csv", resource.getName());
        assertEquals(772, resource.getByteSize());
    }

}
