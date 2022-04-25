package de.landsh.opendata.ckan;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CkanAPI {

    private static final Logger log = LoggerFactory.getLogger(CkanAPI.class);
    private final String baseURL;
    private final ApiKey apiKey;
    private RestClient restClient;

    CkanAPI() {
        baseURL = null;
        apiKey = null;
    }

    public CkanAPI(final String baseURL, final ApiKey apiKey) {
        this.apiKey = apiKey;
        this.baseURL = baseURL;

        int timeout = 10;
        final RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000).build();
        final CloseableHttpClient client =
                HttpClientBuilder.create().setDefaultRequestConfig(config).build();

        restClient = new HttpRestClient(client, HttpClientContext.create());
    }

    /**
     * If the API is additionally protected with HTTP Basic authentication (e.g. a stage system) you can set username
     * and password as parameter.
     */
    public CkanAPI(final String baseURL, final ApiKey apiKey, String userName, String password) {
        this.apiKey = apiKey;
        this.baseURL = baseURL;

        int timeout = 10;
        final RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000).build();

        final CredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
        provider.setCredentials(AuthScope.ANY, credentials);

        final CloseableHttpClient client =
                HttpClientBuilder.create()
                        .setDefaultRequestConfig(config)
                        .setDefaultCredentialsProvider(provider)
                        .build();

        restClient = new HttpRestClient(client, HttpClientContext.create());
    }

    /**
     * Calculate a file's checksum.
     */
    private static String getFileChecksum(MessageDigest digest, File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        byte[] byteArray = new byte[1024];
        int bytesCount = 0;
        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        }

        fis.close();

        byte[] bytes = digest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte aByte : bytes) {
            sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }

    public void setRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Noch (März 2020) kann man das neuste Dataset einer Collection nicht per CKAN-API bestimmen sondern muss
     * über die Weboberfläche gehen.
     *
     * @return packageId des neuesten Dataset
     */
    public String findNewestDataset(String collectionId) throws IOException {
        HttpGet request = new HttpGet(baseURL + "/collection/" + collectionId + "/aktuell");
        HttpParams params = new BasicHttpParams();
        params.setParameter(ClientPNames.HANDLE_REDIRECTS, false);
        request.setParams(params);
        CloseableHttpResponse response = restClient.executeRawHttpRequest(request);

        Header header = response.getFirstHeader("Location");

        String redirectURL = header == null ? null : header.getValue();

        String packageId = StringUtils.substringAfterLast(redirectURL, "/");

        response.close();

        return packageId;
    }

    public JSONObject readDataset(String packageId) throws IOException {
        HttpGet httpGet = new HttpGet(baseURL + "/api/3/action/package_show?id=" + packageId);
        JSONObject responseJSON = restClient.executeHttpRequest(httpGet);
        if (responseJSON.has("result")) {
            return responseJSON.getJSONObject("result");
        } else {
            return null;
        }
    }

    /**
     * Return the first accessURL of a dataset's resource.
     */
    public String getAccessURL(String packageId) throws IOException {
        JSONObject dataset = readDataset(packageId);

        Resource resource = getResource(dataset, true);

        if (resource == null) return null;

        return resource.accessURL;

    }

    /**
     * Ordnet ein Dataset in eine Kollektion ein.
     */
    public boolean putDatasetInCollection(String datasetId, String collectionId) throws IOException {

        if (readDataset(datasetId) == null) {
            throw new IllegalArgumentException("There is not dataset with id " + datasetId);
        }

        final JSONObject collection = readDataset(collectionId);

        if (collection == null) {
            throw new IllegalArgumentException("There is not collection with id " + collectionId);
        }

        if (!"collection".equals(collection.getString("type"))) {
            throw new IllegalArgumentException(collectionId + " is no collection.");
        }

        final JSONObject json = new JSONObject();
        json.put("subject", collectionId);
        json.put("type", "parent_of");
        json.put("object", datasetId);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/3/action/package_relationship_create");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);
        return isResponseSuccess(responseJSON);
    }

    /**
     * Entfernt ein Dataset aus einer Kollektion.
     */
    public boolean removeDatasetFromCollection(String datasetId, String collectionId) throws IOException {

        if (readDataset(datasetId) == null) {
            throw new IllegalArgumentException("There is not dataset with id " + datasetId);
        }

        final JSONObject collection = readDataset(collectionId);

        if (collection == null) {
            throw new IllegalArgumentException("There is not collection with id " + collectionId);
        }

        if (!"collection".equals(collection.getString("type"))) {
            throw new IllegalArgumentException(collectionId + " is no collection.");
        }

        final JSONObject json = new JSONObject();
        json.put("subject", collectionId);
        json.put("type", "parent_of");
        json.put("object", datasetId);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/3/action/package_relationship_delete");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);
        return isResponseSuccess(responseJSON);
    }


    public boolean updatePackage(JSONObject json) throws IOException {

        if (!json.has("id")) {
            throw new IllegalArgumentException("Dataset without id");
        }

        String packageId = json.getString("id");

        HttpPost requestPackageUpdate = new HttpPost(baseURL + "/api/3/action/package_update?id=" + packageId);
        requestPackageUpdate.addHeader("Authorization", apiKey.toString());
        requestPackageUpdate.addHeader("Content-Type", "application/json");
        requestPackageUpdate.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(requestPackageUpdate);
        return isResponseSuccess(responseJSON);
    }

    public String createPackage(JSONObject json) throws IOException {
        HttpPost httpPost = new HttpPost(baseURL + "/api/3/action/package_create");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        if (!responseJSON.getBoolean("success")) {
            throw new RuntimeException(responseJSON.get("error").toString());
        }

        return responseJSON.getJSONObject("result").getString("id");
    }

    public String createCollection(String collectionId, String collectionTitle, String organizationId) throws IOException {

        if (StringUtils.isBlank(collectionId)) {
            throw new IllegalArgumentException("Collection id must not be blank.");
        }

        // Zunächst nachsehen, ob es die Collection bereits gibt.
        final JSONObject collectionObject = readDataset(collectionId);
        if (collectionObject != null) {
            return collectionObject.getString("id");
        }

        log.info("Creating collection {}", collectionId);

        JSONObject json = new JSONObject();
        json.put("name", collectionId);
        json.put("title", collectionTitle);
        json.put("type", "collection");
        json.put("owner_org", organizationId);

        HttpPost httpPost = new HttpPost(baseURL + "/api/3/action/package_create");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);
        return responseJSON.getJSONObject("result").getString("id");
    }

    /**
     * Prüft ob die Antwort von CKAN erfoglreich ist.
     */
    boolean isResponseSuccess(JSONObject response) {
        return response != null && response.has("success") && response.getBoolean("success");
    }

    /**
     * Get the id of a collection this dataset is part of or <code>null</code> if the dataset does not belong to a collection.
     * If the dataset should be part of more than one collection, only the first id will be returned.
     */
    public String getCollection(String packageId) throws IOException {
        final HttpGet requestPackageShow = new HttpGet(baseURL + "/api/3/action/package_show?id=" + packageId);
        final JSONObject response = restClient.executeHttpRequest(requestPackageShow);

        if (response.has("result")) {
            final JSONArray relationships = response.getJSONObject("result").getJSONArray("relationships_as_subject");
            for (Object it : relationships) {
                final JSONObject relationship = (JSONObject) it;
                final JSONObject extras = relationship.getJSONObject("__extras");

                if (extras.isNull("object_package_id")) return null;
                return extras.getString("object_package_id");
            }
        }
        return null;
    }

    public String getOrganization(final String packageId) throws IOException {
        final JSONObject dataset = readDataset(packageId);
        if (dataset == null) {
            return null;
        }

        return dataset.getJSONObject("organization").getString("id");
    }

    public String createResource(JSONObject json) throws IOException {
        final HttpPost httpPost = new HttpPost(baseURL + "/api/action/resource_create");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        if (!responseJSON.getBoolean("success")) {
            throw new RuntimeException(responseJSON.get("error").toString());
        }
        return responseJSON.getJSONObject("result").getString("id");
    }

    public boolean uploadFile(final String packageId, final File file, final String name, final String format,
                              final String mimeType) throws IOException, NoSuchAlgorithmException {
        final MessageDigest shaDigest = MessageDigest.getInstance("MD5");
        final String checksum = getFileChecksum(shaDigest, file);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/action/resource_create");
        httpPost.addHeader("Authorization", apiKey.toString());

        final MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.setCharset(StandardCharsets.UTF_8);
        builder.addPart("package_id", new StringBody(packageId, ContentType.MULTIPART_FORM_DATA));
        builder.addPart("name", new StringBody(name, ContentType.create("multipart/form-data", StandardCharsets.UTF_8)));
        builder.addPart("format", new StringBody(format, ContentType.MULTIPART_FORM_DATA));
        builder.addPart("upload", new FileBody(file, ContentType.create(mimeType), name));
        builder.addPart("hash", new StringBody(checksum, ContentType.MULTIPART_FORM_DATA));
        builder.addPart("mimetype", new StringBody(mimeType, ContentType.MULTIPART_FORM_DATA));
        HttpEntity entity = builder.build();

        log.debug("Sending file {}...", file.getName());
        httpPost.setEntity(entity);
        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        boolean success = isResponseSuccess(responseJSON);
        if (!success) {
            log.error(Objects.toString(responseJSON.get("error")));
        }

        return success;
    }

    public List<Resource> getResources(JSONObject dataset) {
        List<Resource> result = new ArrayList<>();

        JSONArray resources = dataset.has("resources") ?
                dataset.getJSONArray("resources") :
                dataset.getJSONObject("result").getJSONArray("resources");

        for (Object o : resources) {
            final JSONObject resourceJSON = (JSONObject) o;
            final Resource resource = new Resource();
            if (resourceJSON.has("access_url")) {
                resource.setAccessURL(resourceJSON.getString("access_url"));
            } else {
                resource.setAccessURL(resourceJSON.getString("url"));
            }
            resource.setName(resourceJSON.getString("name"));
            resource.setChecksum(StringUtils.trimToNull(resourceJSON.getString("hash")));
            resource.setFormat(resourceJSON.getString("format"));
            if (resourceJSON.has("mimetype") && !resourceJSON.isNull("mimetype")) {
                resource.setMimeType(resourceJSON.getString("mimetype"));
            }
            resource.setId(resourceJSON.getString("id"));

            resource.setByteSize(NumberUtils.toLong(Objects.toString(resourceJSON.get("size"))));

            result.add(resource);
        }

        return result;
    }

    /**
     * Sucht aus dem JSON Objekt eines Datasets die erste Resource/Distribution heraus.
     *
     * @param failIfMoreThanOne werfe eine RuntimeException, wenn es mehr als eine Resource gibt.
     * @return befüllte {@link Resource} Instanz
     */
    public Resource getResource(JSONObject dataset, boolean failIfMoreThanOne) {

        final List<Resource> resources = getResources(dataset);

        if (resources.size() > 1 && failIfMoreThanOne) {
            throw new RuntimeException("Dataset must have exactly one resource");
        }

        if (resources.isEmpty()) {
            return null;
        } else {
            return resources.get(0);
        }

    }

    boolean doesDatasetExist(String packageId) throws IOException {
        final HttpGet requestPackageShow = new HttpGet(baseURL + "/api/3/action/package_show?id=" + packageId);
        final JSONObject response = restClient.executeHttpRequest(requestPackageShow);

        return isResponseSuccess(response);
    }

    public boolean changeTitle(String packageId, String title) throws IOException {
        final JSONObject packageObject = readDataset(packageId);

        if (packageObject == null) {
            log.error("Es gibt kein Dataset mit dem Namen {}", packageId);
            return false;
        }

        packageObject.put("title", title);
        return updatePackage(packageObject);
    }

    public boolean deleteResource(String resourceId) throws IOException {
        final JSONObject json = new JSONObject();
        json.put("id", resourceId);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/action/resource_delete");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        return responseJSON.getBoolean("success");
    }

    public boolean makePackagePrivate(String packageId) throws IOException {
        final String organization = getOrganization(packageId);
        final JSONObject json = new JSONObject();
        json.put("datasets", packageId);
        json.put("org_id", organization);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/action/bulk_update_private");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        return responseJSON.getBoolean("success");
    }

    public boolean makePackagePublic(String packageId) throws IOException {
        final String organization = getOrganization(packageId);
        final JSONObject json = new JSONObject();
        json.put("datasets", packageId);
        json.put("org_id", organization);

        final HttpPost httpPost = new HttpPost(baseURL + "/api/action/bulk_update_public");
        httpPost.addHeader("Authorization", apiKey.toString());
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        final JSONObject responseJSON = restClient.executeHttpRequest(httpPost);

        return responseJSON.getBoolean("success");
    }
}
