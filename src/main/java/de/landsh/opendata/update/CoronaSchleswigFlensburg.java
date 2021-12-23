package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class CoronaSchleswigFlensburg implements Generator {
    private final Set<String> seenURLs = new HashSet<>();
    private final Set<String> seenDates = new HashSet<>();
    private final File targetFile;
    Logger log = LoggerFactory.getLogger(CoronaSchleswigFlensburg.class);
    String BASE_URL = "https://www.schleswig-flensburg.de";
    private PrintStream out;

    public CoronaSchleswigFlensburg(String id, DatasetUpdate update) {
        targetFile = new File( update.getGeneratorArgs().get("targetFile"));
    }

    private void work() throws IOException {
        final String url = "https://www.schleswig-flensburg.de/Leben-Soziales/Gesundheit/Coronavirus/Aktuelle-Zahlen/";

        if (targetFile.exists()) {
            out = new PrintStream(new FileOutputStream(targetFile, true));
            readExistingDates();
        } else {
            out = new PrintStream(new FileOutputStream(targetFile));
            out.println("Datum,Amt/Gemeinde,Aktive Quarantänen,Positiv Getestete");
        }

        processDocument(url);
        out.close();
    }


    /**
     * Liest die Datei <code>targetFile</code> und speichere vorhandene Datumsangaben in <code>seenDates</code>.
     */
    private void readExistingDates() throws IOException {
        final BufferedReader in = new BufferedReader(new FileReader(targetFile));
        in.readLine(); // skip header row
        String line = in.readLine();
        while (line != null) {
            final String date = StringUtils.substringBefore(line, ",");
            seenDates.add(date);
            line = in.readLine();
        }
        in.close();
    }

    private void processDocument(String url) throws IOException {
        log.debug("Processing {}", url);
        final Document doc = Jsoup.connect(url).get();
        final Element div = doc.getElementById("readthis");

        if (div.select(".accordion-container").isEmpty()) {
            processTable(div);
        } else {
            // Einstiegsseite
            processTable(div.select(".accordion-container").first());
        }

        final Elements links = div.select("a");
        for (final Element link : links) {
            final String text = link.text();
            final String href = URLDecoder.decode(link.attr("href"), "utf-8");

            if (text.startsWith("Zu den Zahlen vom") && !seenURLs.contains(href)) {
                final String date = StringUtils.substringAfter(text, "Zu den Zahlen vom ");
                if (!seenDates.contains(date)) {
                    seenURLs.add(href);
                    processDocument(BASE_URL + href);
                }
            }

        }
    }

    private void processTable(Element div) {
        final Elements rows = div.select("tr");
        boolean isFirst = true;
        String date = "";
        for (final Element row : rows) {
            final Elements tds = row.select("td");
            final String cell1 = tds.get(0).text();
            final String cell2 = tds.get(1).text();
            final String cell3 = tds.get(2).text();
            if (isFirst) {
                isFirst = false;
                date = StringUtils.substringBefore(cell1, ",");
                if (seenDates.contains(date)) {
                    return;
                }
                if (!"Aktive Quarantänen".equals(cell2)) {
                    throw new RuntimeException("In Spalte 2 sollte 'Aktive Quarantänen' stehen, dort steht aber " + cell2);
                }
                if (!"Positiv Getestete".equals(cell3)) {
                    throw new RuntimeException("In Spalte 3 sollte 'Positiv Getestete' stehen, dort steht aber " + cell3);
                }

            } else {
                out.println(date + "," + cell1 + "," + cell2 + "," + cell3);
            }
        }
        out.flush();
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        this.work();
        Path source = targetFile.toPath();
        Path target = new File(directory, "corona.csv").toPath();
        Files.copy(source, target);
        return true;
    }
}
