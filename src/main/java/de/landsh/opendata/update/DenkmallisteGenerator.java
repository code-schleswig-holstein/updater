package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DenkmallisteGenerator implements Generator {
    public static final Logger log = LoggerFactory.getLogger(DenkmallisteGenerator.class);

    private final static Map<String, byte[]> cache = new HashMap<>();
    private final String countyName;
    private final String jsonURL;
    private final String username;
    private final String password;
    private final Generator pdfDownload;
    private final File photoDirectory;
    private final String photoBaseURL;

    public DenkmallisteGenerator(DatasetUpdate update) {
        final String pdfURL = update.getGeneratorArgs().get("pdf");
        final String photoDirectoryName = update.getGeneratorArgs().get("photoDirectory");
        this.countyName = update.getGeneratorArgs().get("county");
        this.jsonURL = update.getGeneratorArgs().get("json");
        this.username = update.getGeneratorArgs().get("username");
        this.password = update.getGeneratorArgs().get("password");
        this.photoBaseURL = update.getGeneratorArgs().get("photoBaseURL");

        if (StringUtils.isNotBlank(photoDirectoryName)) {
            final File dir = new File(photoDirectoryName);
            if (dir.exists() && dir.isDirectory() && dir.canRead()) {
                photoDirectory = dir;
            } else {
                log.warn("Photo directory '{}' is not a readable directory.", photoDirectoryName);
                photoDirectory = null;
            }
        } else {
            photoDirectory = null;
        }

        if (StringUtils.isNotBlank(pdfURL)) {
            final DatasetUpdate pdfUpdate = new DatasetUpdate();
            pdfUpdate.setOriginalURL(pdfURL);
            pdfUpdate.setPassword(password);
            pdfUpdate.setUsername(username);
            pdfDownload = new JustDownloadGenerator(countyName + ".pdf", pdfUpdate);
        } else {
            pdfDownload = new NoopGenerator();
        }
    }

    private static void cleanArray(JSONObject entry, String key) {
        if (entry.has(key)) {
            if (StringUtils.isBlank(entry.get(key).toString())) {
                entry.remove(key);
            } else if (entry.get(key) instanceof String) {
                final String text = entry.getString(key);
                entry.remove(key);
                final JSONArray array = new JSONArray();
                array.put(text);
                entry.put(key, array);
            }
        }
    }

    private static void writeCsv(JSONArray entries, Writer writer) throws IOException {
        writer.write("Objektnummer;Kulturdenkmaltyp;Kreis;Gemeinde;Adresse-Lage;Bezeichnung;Beschreibung;Begründung;Schutzumfang;FotoURL\n");
        for (int i = 0; i < entries.length(); i++) {
            final JSONObject entry = entries.getJSONObject(i);

            writer.write(Integer.toString(entry.getInt("Objektnummer")));
            writer.write(";\"");

            if (entry.has("Kulturdenkmaltyp")) {
                writer.write(entry.getString("Kulturdenkmaltyp"));
            }
            writer.write("\";\"");
            writer.write(entry.getString("Kreis"));
            writer.write("\";\"");
            writer.write(entry.getString("Gemeinde"));
            writer.write("\";\"");
            if (entry.has("Adresse-Lage"))
                writer.write(entry.getString("Adresse-Lage"));
            writer.write("\";\"");
            writer.write(entry.getString("Bezeichnung"));
            writer.write("\";\"");
            writer.write(entry.getString("Beschreibung"));
            writer.write("\";\"");
            if (entry.has("Begründung")) {
                writer.write(entry.getJSONArray("Begründung").toList().stream().map(Object::toString).collect(Collectors.joining(", ")));
            }
            writer.write("\";\"");
            if (entry.has("Schutzumfang")) {
                writer.write(entry.getJSONArray("Schutzumfang").toList().stream().map(Object::toString).collect(Collectors.joining(", ")));
            }
            writer.write("\";\"");
            if (entry.has("FotoURL")) {
                writer.write(entry.getString("FotoURL"));
            }


            writer.write("\"\r\n");
        }
        writer.close();
    }

    private byte[] downloadJSONfile() throws IOException {
        if (cache.containsKey(jsonURL)) {
            return cache.get(jsonURL);
        } else {
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

            final CloseableHttpResponse response = client.execute(new HttpGet(jsonURL));

            if (response.getStatusLine().getStatusCode() == 200) {
                final byte[] data = EntityUtils.toByteArray(response.getEntity());
                response.close();
                cache.put(jsonURL, data);
                return data;
            }
        }

        throw new FileNotFoundException(jsonURL);
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {

        // PDF-Datei einfach herunterladen.
        pdfDownload.generateDistributions(directory);

        processXmlFile(new ByteArrayInputStream(downloadJSONfile()), directory, countyName);

        return true;
    }

    private void processXmlFile(InputStream is, File targetDirectory, String countyName) throws IOException {
        is.read(new byte[3]); // skip BOM

        final JSONArray list = new JSONArray(new JSONTokener(is));
        final Map<String, JSONArray> perCounty = new HashMap<>();

        for (int i = 0; i < list.length(); i++) {
            final JSONObject entry = list.getJSONObject(i);
            final int objectNumber = entry.getInt("Objektnummer");

            entry.remove("Volltext");
            entry.remove("Index");
            entry.remove("lng");
            entry.remove("lat");
            entry.remove("FotoUrl");

            if (entry.has("Adresse-Lage")) {
                if (StringUtils.isBlank(entry.getString("Adresse-Lage"))) {
                    entry.remove("Adresse-Lage");
                } else {
                    entry.put("Adresse-Lage", entry.getString("Adresse-Lage").trim());
                }
            }
            if (entry.has("Kulturdenkmaltyp ")) {
                entry.put("Kulturdenkmaltyp", entry.getString("Kulturdenkmaltyp ").trim());
                entry.remove("Kulturdenkmaltyp ");
            }

            if (entry.has("Kulturdenkmaltyp")) {
                if (StringUtils.isBlank(entry.getString("Kulturdenkmaltyp"))) {
                    // 2021-04-01 Workaround für fehlende Angaben bei Schutzzonen und beweglichen Kulturdenkmalen
                    if (ArrayUtils.contains(new int[]{13298, 13299, 13300, 13301, 29353, 33001}, objectNumber)) {
                        entry.put("Kulturdenkmaltyp", "Schutzzone");
                    } else {
                        entry.put("Kulturdenkmaltyp", "Bewegliches Kulturdenkmal");
                    }
                }
            }

            cleanArray(entry, "Begründung");
            cleanArray(entry, "Schutzumfang");

            if (photoDirectory != null) {
                final File photo = new File(photoDirectory, objectNumber + ".jpg");
                if (photo.exists()) {
                    entry.put("FotoURL", photoBaseURL + objectNumber + ".jpg");
                }
            }

            final String county = entry.getString("Kreis");
            if (!perCounty.containsKey(county)) {
                perCounty.put(county, new JSONArray());
            }

            perCounty.get(county).put(entry);
        }

        if (StringUtils.isBlank(countyName)) {
            // Einträge für ganz Schleswig-Holstein sammeln
            final JSONArray all = new JSONArray();
            for (JSONArray jsonArray : perCounty.values()) {
                for (Object it : jsonArray) {
                    all.put(it);
                }
            }
            final FileWriter writer = new FileWriter(new File(targetDirectory, "denkmalliste.json"));
            all.write(writer, 2, 1);
            writer.close();

            // CSV-Dateien schreiben
            writeCsv(all, new FileWriter(new File(targetDirectory, "denkmalliste.csv"), StandardCharsets.UTF_8));
            writeCsv(all, new FileWriter(new File(targetDirectory, "denkmalliste-latin1.csv"), StandardCharsets.ISO_8859_1));
        } else {
            final FileWriter writer = new FileWriter(new File(targetDirectory, countyName + ".json"));

            perCounty.get(countyName).write(writer);
            writer.close();

            // CSV-Dateien schreiben
            final JSONArray entries = perCounty.get(countyName);
            writeCsv(entries, new FileWriter(new File(targetDirectory, countyName + ".csv"), StandardCharsets.UTF_8));
            writeCsv(entries, new FileWriter(new File(targetDirectory, countyName + "-latin1.csv"), StandardCharsets.ISO_8859_1));
        }

    }
}
