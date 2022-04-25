package de.landsh.opendata.ckan;

import lombok.RequiredArgsConstructor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

@RequiredArgsConstructor
public class HttpRestClient implements RestClient {

    private final CloseableHttpClient client;
    private final HttpClientContext context;

    @Override
    public JSONObject executeHttpRequest(HttpUriRequest request) throws IOException {
        final CloseableHttpResponse response = client.execute(request, context);
        final String rawJSON = EntityUtils.toString(response.getEntity());

        response.close();
        try {
            return new JSONObject(rawJSON);
        } catch (JSONException e) {
            throw new RuntimeException("Invalid response from CKAN server: " + rawJSON);
        }
    }

    @Override
    public CloseableHttpResponse executeRawHttpRequest(HttpUriRequest request) throws IOException {
        return client.execute(request, context);
    }


}
