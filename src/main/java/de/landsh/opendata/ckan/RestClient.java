package de.landsh.opendata.ckan;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Diese Klasse abstrahiert den HTTP Aufruf eines JSON Douments.
 */
public interface RestClient {
    JSONObject executeHttpRequest(HttpUriRequest request) throws IOException;

    CloseableHttpResponse executeRawHttpRequest(HttpUriRequest request) throws IOException;

}
