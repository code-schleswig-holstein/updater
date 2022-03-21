package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import de.landsh.opendata.OpenDataUpdatesCkan;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Ladesaeulenregister implements Generator {

    private static final String TARGET_FILE = "ladesaeulenregister.csv";
    private static final String TEMP_FILE = "raw.csv";
    private static final Logger log = LoggerFactory.getLogger(Ladesaeulenregister.class);
    private final String url;

    public Ladesaeulenregister(DatasetUpdate update) {
        this.url = update.getOriginalURL();
    }

    /**
     * Zunächst muss von der Startseite der Name der aktuellen Excel-Datei bestimmt werden.
     *
     * @return vollständigen URL der Excel-Datei
     */
    String determineExcelFileURL() throws IOException {
        final CloseableHttpClient client = HttpClientBuilder.create().build();
        final CloseableHttpResponse response = client.execute(new HttpGet(url));

        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        String url = null;

        String line = reader.readLine();
        while (line != null) {
            if (line.contains("Ladesaeulenregister.xlsx") && line.contains("blob=publicationFile")) {
                url = StringUtils.substringBetween(line, "href=\"", "\"");
            }
            line = reader.readLine();
        }

        reader.close();
        response.close();
        client.close();

        final String scheme = StringUtils.substringBefore(this.url, "://");
        final String hostPort = StringUtils.substringBetween(this.url, "://", "/");

        return scheme + "://" + hostPort + url;
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {

        final String excelURL = determineExcelFileURL();

        // Excel-Datei herunterladen und als CSV umwandeln
        final DatasetUpdate excelDownload = new DatasetUpdate();
        excelDownload.setOriginalURL(excelURL);
        final Excel2CsvGenerator excel2CsvGenerator = new Excel2CsvGenerator("raw", excelDownload);
        boolean downloadSuccess = excel2CsvGenerator.generateDistributions(directory);

        if (!downloadSuccess) {
            return false;
        }

        final File rawCsvFile = new File(directory, TEMP_FILE);

        final File targetFile = new File(directory, TARGET_FILE);
        final PrintWriter out = new PrintWriter(new FileWriter(targetFile));

        boolean inData = false;
        final BufferedReader reader = new BufferedReader(new FileReader(rawCsvFile));
        String line = reader.readLine();
        while (line != null) {
            if (inData) {
                if (line.contains(",Schleswig-Holstein,")) {
                    out.println(line);
                }
            } else {
                if (line.startsWith("Betreiber,Straße")) {
                    out.println(line);
                    inData = true;
                } else if (line.startsWith("Stand:")) {
                    String rawDate = StringUtils.trim(StringUtils.substringAfter(line, "Stand:"));
                    LocalDate date = LocalDate.parse(rawDate, DateTimeFormatter.ofPattern("dd.MM.yyyy"));
                    Files.writeString( new File(directory, OpenDataUpdatesCkan.METADATA_FILE_TIME_START).toPath(),date.format(DateTimeFormatter.ISO_DATE));
                    Files.writeString( new File(directory, OpenDataUpdatesCkan.METADATA_FILE_TIME_END).toPath(),date.format(DateTimeFormatter.ISO_DATE));
                }
            }

            line = reader.readLine();
        }
        out.close();
        reader.close();

        if (!rawCsvFile.delete()) {
            log.warn("Could not delete temporary CSV file {}", rawCsvFile);
        }

        return true;
    }
}
