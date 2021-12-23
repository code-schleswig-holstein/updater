package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Dieser Generator lädt einfach nur eine Datei herunter.
 */
public class JustDownloadGenerator implements Generator {

    private final String url;
    private final String distributionName;
    private final String password;
    private final String username;

    public JustDownloadGenerator(String distributionName, DatasetUpdate update) {
        this.url = update.getOriginalURL();
        this.distributionName = distributionName;
        this.username = update.getUsername();
        this.password = update.getPassword();
    }

    @Override
    public boolean generateDistributions(File directory) throws IOException {
        final File file = new File(directory, distributionName);

        final CloseableHttpClient client;
        if (username != null && password != null) {
            final CredentialsProvider provider = new BasicCredentialsProvider();
            final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
            provider.setCredentials(AuthScope.ANY, credentials);

            client = HttpClientBuilder.create()
                    .setDefaultCredentialsProvider(provider)
                    .build();
        } else {
            client = HttpClientBuilder.create().build();
        }

        final CloseableHttpResponse response = client.execute(new HttpGet(url));

        if (response.getStatusLine().getStatusCode() == 200) {

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                FileOutputStream outstream = new FileOutputStream(file);
                entity.writeTo(outstream);
                outstream.close();
            }
            response.close();
            return true;
        }

        return false;
    }
}
