package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Umwandlung der Kommunalen Wappenrolle
 * <p>
 * Der Export ist unter der Adresse
 * https://efi2.schleswig-holstein.de/wr/wr_opendata.xml
 * zu erreichen - in einem sehr merkwürdigen XML-Format.
 * <p>
 * Die "Übersetzung" von interner Wappennummer auf Gemeinde-URI befindet sich in der Datei
 * /src/main/resources/wappenrolleGeocoding.csv
 */
public class WappenrolleGenerator implements Generator {

    private static final Logger log = LoggerFactory.getLogger(WappenrolleGenerator.class);
    private final String type;
    private final Map<Integer, String> geocoding = new HashMap<>();
    private final JSONArray wappen = new JSONArray();
    private final JSONArray flaggen = new JSONArray();
    private final List<String> xmlWappen = new LinkedList<>();
    private final List<String> xmlFlaggen = new LinkedList<>();
    private final String url;

    public WappenrolleGenerator(DatasetUpdate update) {
        this.type = update.getGeneratorArgs().get("type");
        this.url = update.getOriginalURL();
    }

    static String convertDate(String germanDate) {
        if (StringUtils.trimToNull(germanDate) == null) {
            return null;
        }

        String[] s = germanDate.trim().split("\\.");

        return s[2] + "-" + s[1] + "-" + s[0];
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        readId2Geocoding();

        final InputStream in = new URL(url).openStream();

        final XMLInputFactory factory = XMLInputFactory.newInstance();
        final XMLStreamReader parser = factory.createXMLStreamReader(in);

        Map<String, String> entry = null;
        String tag = null;
        final StringBuilder sb = new StringBuilder();

        while (parser.hasNext()) {
            if (parser.getEventType() == XMLStreamConstants.START_ELEMENT) {
                tag = parser.getLocalName();
                sb.setLength(0);

                if ("ID".equals(parser.getLocalName())) {
                    // Es beginnt ein neues Wappen.

                    if (entry != null) {
                        processEntry(entry);
                    }

                    entry = new HashMap<>();
                }
            } else if (parser.getEventType() == XMLStreamConstants.END_ELEMENT) {
                if (tag != null && entry != null) {
                    entry.put(tag, sb.toString());
                }
            } else if (parser.getEventType() == XMLStreamConstants.CHARACTERS) {
                if (!parser.isWhiteSpace()) {

                    // Zeichenkodierung reparieren  - ist im Juni 2020 nicht mehr notwendig, da die XML-Datei repariert wurde
                    //sb.append(new String(parser.getText().getBytes(java.nio.charset.StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
                    sb.append(parser.getText());
                }
            }


            parser.next();
        }

        if (entry != null) {
            processEntry(entry);
        }

        if ("wappen".equals(type)) {
            writeToFile(wappen, new File(directory, "wappen.json"));
            writeToFile(xmlWappen, new File(directory, "wappen.rdf"));
        } else {
            writeToFile(flaggen, new File(directory, "flaggen.json"));
            writeToFile(xmlFlaggen, new File(directory, "flaggen.rdf"));
        }

        return true;
    }

    private void writeToFile(JSONArray jsonArray, File file) throws IOException {
        final FileWriter writer = new FileWriter(file);
        jsonArray.write(writer);
        writer.close();
    }

    private void writeToFile(List<String> xmlList, File file) throws IOException {
        final PrintWriter writer = new PrintWriter(new FileWriter(file));
        writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.println("<rdf:RDF xmlns:wr=\"https://efi2.schleswig-holstein.de/wr/\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:foaf=\"http://xmlns.com/foaf/0.1/\">");
        for (String xml : xmlList) {
            writer.println(xml);
        }
        writer.println("</rdf:RDF>");
        writer.close();
    }

    /**
     * Zuordnung von Wappenrollen-Id zu Geocoding laden.
     */
    private void readId2Geocoding() throws IOException {
        final BufferedReader in = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/wappenrolleGeocoding.csv")));
        for (String line = in.readLine(); line != null; line = in.readLine()) {
            final int id = NumberUtils.toInt(StringUtils.substringBefore(line, ","));
            final String uri = StringUtils.substringAfter(line, ",");
            if (id > 0 && StringUtils.isNotEmpty(uri)) {
                geocoding.put(id, uri);
            }
        }
        in.close();
    }

    private void processEntry(Map<String, String> entry) {
        if (StringUtils.isNotEmpty(entry.get("Flaggenbeschreibung"))) {
            final JSONObject json = commonJson(entry);
            final StringBuilder xml = new StringBuilder();
            xml.append("<wr:Flag rdf:about=\"https://opendata.schleswig-holstein.de/lod/wr/flagge/")
                    .append(entry.get("ID"))
                    .append("\">\n");
            json.put("uri", "https://opendata.schleswig-holstein.de/lod/wr/flagge/" + entry.get("ID"));

            if (StringUtils.isNotEmpty(entry.get("Flaggengenehmigung"))) {
                json.put("acceptance", convertDate(entry.get("Flaggengenehmigung")));
                xml.append("<wr:acceptance  rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">")
                        .append(convertDate(entry.get("Flaggengenehmigung")))
                        .append("</wr:acceptance>");
            }

            json.put("description", entry.get("Flaggenbeschreibung"));
            xml.append("<wr:description>")
                    .append(StringEscapeUtils.escapeXml10(entry.get("Flaggenbeschreibung")))
                    .append("</wr:description>");

            if (StringUtils.isNotBlank(entry.get("txtBILD-F"))) {
                json.put("img", "https://efi2.schleswig-holstein.de/wr/images/" + entry.get("txtBILD-F"));
                xml.append("<foaf:img rdf:resource=\"https://efi2.schleswig-holstein.de/wr/images/").append(entry.get("txtBILD-F")).append("\" />");
            }

            flaggen.put(json);

            appendCommonXml(xml, entry);

            xml.append("</wr:Flag>");

            xmlFlaggen.add(xml.toString());
        }

        if (StringUtils.isNotEmpty(entry.get("Wappenbeschreibung"))) {
            final JSONObject json = commonJson(entry);

            final StringBuilder xml = new StringBuilder();
            xml.append("<wr:CoatOfArms rdf:about=\"https://opendata.schleswig-holstein.de/lod/wr/wappen/")
                    .append(entry.get("ID"))
                    .append("\">\n");
            json.put("uri", "https://opendata.schleswig-holstein.de/lod/wr/wappen/" + entry.get("ID"));

            if (StringUtils.isNotBlank(entry.get("txtBILD-W"))) {
                json.put("img", "https://efi2.schleswig-holstein.de/wr/images/" + entry.get("txtBILD-W"));
                xml.append("<foaf:img rdf:resource=\"https://efi2.schleswig-holstein.de/wr/images/")
                        .append(entry.get("txtBILD-W")).append("\" />");
            }

            if (StringUtils.isNotBlank(entry.get("Wappengenehmigung"))) {
                json.put("acceptance", convertDate(entry.get("Wappengenehmigung")));
                xml.append("<wr:acceptance  rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">")
                        .append(convertDate(entry.get("Wappengenehmigung")))
                        .append("</wr:acceptance>");
            }
            json.put("description", entry.get("Wappenbeschreibung"));
            xml.append("<wr:description>")
                    .append(StringEscapeUtils.escapeXml11(entry.get("Wappenbeschreibung")))
                    .append("</wr:description>");

            wappen.put(json);

            appendCommonXml(xml, entry);
            xml.append("</wr:CoatOfArms>");
            xmlWappen.add(xml.toString());

        }
    }

    private void appendCommonXml(StringBuilder xml, Map<String, String> entry) {
        final int id = Integer.parseInt(entry.get("ID"));
        if (geocoding.containsKey(id)) {
            xml.append("<wr:municipality rdf:resource=\"").append(geocoding.get(id)).append("\" />");
        }
        xml.append("<wr:municipalityName>").append(entry.get("Kommune")).append("</wr:municipalityName>");
        xml.append("<wr:historicalJustification>").append(StringEscapeUtils.escapeXml11(entry.get("HistorischeBegruendung"))).append("</wr:historicalJustification>");

        for (final String i : new String[]{"I", "II", "III", "IV", "V"}) {
            if (StringUtils.isNotEmpty(entry.get("Entwurfsautor" + i))) {
                xml.append("<wr:author><foaf:Person><foaf:name>");
                xml.append(entry.get("Entwurfsautor" + i));
                xml.append("</foaf:name></foaf:Person></wr:author>");
            }
        }

        for (int i = 1; i <= 7; i++) {
            if (StringUtils.isNotEmpty(entry.get("Wappenfigur" + i))) {
                xml.append("<wr:figure>").append(entry.get("Wappenfigur" + i)).append("</wr:figure>");
            }
        }

        if (StringUtils.isNotEmpty(entry.get("LoeschDat"))) {
            xml.append("<wr:cancellation  rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">").append(convertDate(entry.get("LoeschDat"))).append("</wr:cancellation>");
        }
        if (StringUtils.isNotEmpty(entry.get("LoeschGrund"))) {
            xml.append("<wr:cancellationReason>").append(entry.get("LoeschGrund")).append("</wr:cancellationReason>");
        }
    }

    private JSONObject commonJson(Map<String, String> entry) {
        final JSONObject json = new JSONObject();

        int id = Integer.parseInt(entry.get("ID"));

        json.put("id", id);
        json.put("municipalityName", entry.get("Kommune"));

        if (geocoding.containsKey(id)) {
            json.put("municipality", geocoding.get(id));
        } else {
            log.info("missing geocoding for #{}", id);
        }

        json.put("historicalJustification", entry.get("HistorischeBegruendung"));

        if (StringUtils.isNotEmpty(entry.get("EntwurfsautorI")) && !"unbekannt".equalsIgnoreCase(entry.get("EntwurfsautorI"))) {
            final JSONArray authors = new JSONArray();
            authors.put(entry.get("EntwurfsautorI"));
            if (StringUtils.isNotEmpty(entry.get("EntwurfsautorII"))) {
                authors.put(entry.get("EntwurfsautorII"));
            }
            if (StringUtils.isNotEmpty(entry.get("EntwurfsautorIII"))) {
                authors.put(entry.get("EntwurfsautorIII"));
            }
            if (StringUtils.isNotEmpty(entry.get("EntwurfsautorIV"))) {
                authors.put(entry.get("EntwurfsautorIV"));
            }
            if (StringUtils.isNotEmpty(entry.get("EntwurfsautorV"))) {
                authors.put(entry.get("EntwurfsautorV"));
            }
            json.put("author", authors);

            final JSONArray figures = new JSONArray();
            for (int i = 1; i <= 7; i++) {
                if (StringUtils.isNotEmpty(entry.get("Wappenfigur" + i))) {
                    figures.put(entry.get("Wappenfigur" + i));
                }
            }
            json.put("figure", figures);

            if (StringUtils.isNotEmpty(entry.get("LoeschDat"))) {
                json.put("cancellation", convertDate(entry.get("LoeschDat")));
            }
            if (StringUtils.isNotEmpty(entry.get("LoeschGrund"))) {
                json.put("cancellationReason", entry.get("LoeschGrund"));
            }

        }
        return json;
    }
}
