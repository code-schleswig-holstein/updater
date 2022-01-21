package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

public class CoronaSchuldashboard implements Generator {

    private static final String CODE_SCHULE_GESCHLOSSEN = "9ecdbfe5e3";
    private static final String CODE_SCHULE_BEINTRAECHTIGT = "e38957a614";
    private static final String CODE_FAELLE_KREIS = "7b869c37b7";
    private static final String CODE_FAELLE_SCHULART = "9518e0539f";
    private static final String CODE_TEST_ART = "1f99d72f38";
    private static final String CODE_TEST_POSITIV = "1b75fec02d";
    private static final String CODE_TEST_IN_SCHULE = "e9e1a89fb2";
    private static final Logger log = LoggerFactory.getLogger(CoronaSchuldashboard.class);
    private static final String EARLIEST_DATE = "2020-08-10";
    private static final String EARLIEST_WEEK = "2021-W16";
    static String[] AVAILABLE_WEEKS = new String[]{"2022-W02", "2021-W51", "2021-W50", "2021-W49", "2021-W48", "2021-W47", "2021-W46", "2021-W45", "2021-W44", "2021-W43", "2021-W42", "2021-W39", "2021-W38", "2021-W37", "2021-W36", "2021-W35", "2021-W34", "2021-W33", "2021-W32", "2021-W31", "2021-W24", "2021-W23", "2021-W22", "2021-W21", "2021-W20", "2021-W19", "2021-W18", "2021-W17", "2021-W16"};
    /*
    pro Tag seit 2020-08-10
Geschlossene Schulen — Nach Kreisen und Schularten	https://api.public.polyteia.de/data/9ecdbfe5e3/?schultag=2022-01-12
Beeinträchtigte Schulen — Nach Kreisen und Schularten	https://api.public.polyteia.de/data/e38957a614/?schultag=2022-01-12
Anzahl der COVID-19 Fälle (PCR-Test) an Schulen nach Kreisen	https://api.public.polyteia.de/data/7b869c37b7/?schultag=2022-01-12
Anzahl der COVID-19 Fälle (PCR-Test) an Schulen nach Schularten	https://api.public.polyteia.de/data/9518e0539f/?schultag=2022-01-12

pro Woche seit 2021-W16
Anzahl durchgeführter Testungen nach Art des Nachweises	https://api.public.polyteia.de/data/1f99d72f38/?week=2021-W51
Testungen in der Schule mit einem positiven Testergebnis	https://api.public.polyteia.de/data/1b75fec02d/?week=2021-W51
Anzahl durchgeführter Testungen in der Schule	https://api.public.polyteia.de/data/e9e1a89fb2/?week=2021-W51


     */
    static String[] AVAILABLE_DAYS = new String[]{"2022-01-13", "2022-01-12", "2022-01-11", "2022-01-10", "2021-12-22", "2021-12-21", "2021-12-20", "2021-12-17", "2021-12-16", "2021-12-15", "2021-12-14", "2021-12-13", "2021-12-10", "2021-12-09", "2021-12-08", "2021-12-07", "2021-12-06", "2021-12-03", "2021-12-02", "2021-12-01", "2021-11-30", "2021-11-29", "2021-11-26", "2021-11-25", "2021-11-24", "2021-11-23", "2021-11-22", "2021-11-19", "2021-11-18", "2021-11-17", "2021-11-16", "2021-11-15", "2021-11-12", "2021-11-11", "2021-11-10", "2021-11-09", "2021-11-08", "2021-11-05", "2021-11-04", "2021-11-03", "2021-11-02", "2021-11-01", "2021-10-29", "2021-10-28", "2021-10-27", "2021-10-26", "2021-10-25", "2021-10-22", "2021-10-21", "2021-10-20", "2021-10-19", "2021-10-18", "2021-10-01", "2021-09-30", "2021-09-29", "2021-09-28", "2021-09-27", "2021-09-24", "2021-09-23", "2021-09-22", "2021-09-21", "2021-09-20", "2021-09-17", "2021-09-16", "2021-09-15", "2021-09-14", "2021-09-13", "2021-09-10", "2021-09-09", "2021-09-08", "2021-09-07", "2021-09-06", "2021-09-03", "2021-09-02", "2021-09-01", "2021-08-31", "2021-08-30", "2021-08-27", "2021-08-26", "2021-08-25", "2021-08-24", "2021-08-23", "2021-08-20", "2021-08-19", "2021-08-18", "2021-08-17", "2021-08-16", "2021-08-13", "2021-08-12", "2021-08-11", "2021-08-10", "2021-08-09", "2021-08-06", "2021-08-05", "2021-08-04", "2021-08-03", "2021-08-02", "2021-06-18", "2021-06-17", "2021-06-16", "2021-06-15", "2021-06-14", "2021-06-11", "2021-06-10", "2021-06-09", "2021-06-08", "2021-06-07", "2021-06-04", "2021-06-03", "2021-06-02", "2021-06-01", "2021-05-31", "2021-05-28", "2021-05-27", "2021-05-26", "2021-05-25", "2021-05-21", "2021-05-20", "2021-05-19", "2021-05-18", "2021-05-17", "2021-05-12", "2021-05-11", "2021-05-10", "2021-05-07", "2021-05-06", "2021-05-05", "2021-05-04", "2021-05-03", "2021-04-30", "2021-04-29", "2021-04-28", "2021-04-27", "2021-04-26", "2021-04-23", "2021-04-22", "2021-04-21", "2021-04-20", "2021-04-19", "2021-03-31", "2021-03-30", "2021-03-29", "2021-03-26", "2021-03-25", "2021-03-24", "2021-03-23", "2021-03-22", "2021-03-19", "2021-03-18", "2021-03-17", "2021-03-16", "2021-03-15", "2021-03-12", "2021-03-11", "2021-03-10", "2021-03-09", "2021-03-08", "2020-12-15", "2020-12-14", "2020-12-11", "2020-12-10", "2020-12-09", "2020-12-08", "2020-12-07", "2020-12-04", "2020-12-03", "2020-12-02", "2020-12-01", "2020-11-30", "2020-11-27", "2020-11-26", "2020-11-25", "2020-11-24", "2020-11-23", "2020-11-20", "2020-11-19", "2020-11-18", "2020-11-17", "2020-11-16", "2020-11-13", "2020-11-12", "2020-11-11", "2020-11-10", "2020-11-09", "2020-11-06", "2020-11-05", "2020-11-04", "2020-11-03", "2020-11-02", "2020-10-30", "2020-10-29", "2020-10-28", "2020-10-27", "2020-10-26", "2020-10-23", "2020-10-22", "2020-10-21", "2020-10-20", "2020-10-19", "2020-10-02", "2020-10-01", "2020-09-30", "2020-09-29", "2020-09-28", "2020-09-25", "2020-09-24", "2020-09-23", "2020-09-22", "2020-09-21", "2020-09-18", "2020-09-17", "2020-09-16", "2020-09-15", "2020-09-14", "2020-09-11", "2020-09-10", "2020-09-09", "2020-09-08", "2020-09-07", "2020-09-04", "2020-09-03", "2020-09-02", "2020-09-01", "2020-08-31", "2020-08-28", "2020-08-27", "2020-08-26", "2020-08-25", "2020-08-24", "2020-08-21", "2020-08-20", "2020-08-19", "2020-08-18", "2020-08-17", "2020-08-14", "2020-08-13", "2020-08-12", "2020-08-11", "2020-08-10"};
    private final File targetFile;
    private final String code;
    private final String frequency;

    public CoronaSchuldashboard(String id, DatasetUpdate update) {
        targetFile = new File(update.getGeneratorArgs().get("targetFile"));
        code = update.getGeneratorArgs().get("code");
        frequency = update.getGeneratorArgs().get("frequency");
    }

    private static List<List<String>> convertTable(String date, JSONObject json, String ignoreColumn) {
        if (json == null) {
            return null;
        }

        List<List<String>> result = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        JSONArray columns = json.getJSONArray("columns");
        for (int i = 1; i < columns.length(); i++) {
            JSONObject c = columns.getJSONObject(i);
            columnNames.add(c.getString("name"));
        }

        JSONArray rows = json.getJSONArray("rows");
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);

            if (!row.has("type") || !"summary".equals(row.getString("type"))) {

                String[] cells = readRow(row.getJSONArray("cells"));
                if (cells.length != columnNames.size() + 1) {
                    throw new RuntimeException("ungleiche Anzahl Spalten in in Reihe " + (i + 1));
                }

                for (int j = 1; j < cells.length; j++) {
                    String value = cells[j];
                    String columnName = columnNames.get(j - 1);

                    if (!ignoreColumn.equals(columnName)) {

                        final List<String> resultRow = new ArrayList<>();
                        resultRow.add(date);
                        resultRow.add(cells[0]);
                        resultRow.add(columnName);
                        resultRow.add(value);
                        result.add(resultRow);
                    }
                }
            }
        }

        return result;
    }

    static String[] readRow(JSONArray cells) {
        String[] result = new String[cells.length()];
        for (int i = 0; i < cells.length(); i++) {
            result[i] = cells.getJSONObject(i).get("value").toString();
        }
        return result;
    }

    static List<List<String>> convertFaelleNachKreis(String date, JSONObject json) {
        return convertTable(date, json, "Neue Fälle insgesamt");
    }

    static List<List<String>> convertFaelleNachSchulart(String date, JSONObject json) {
        return convertTable(date, json, "Neue Fälle insgesamt");
    }

    static List<List<String>> convertSchuleGeschlossen(String date, JSONObject json) {
        return convertTable(date, json, "Gesamt");
    }

    static List<List<String>> convertSchuleBeeintraechtigt(String date, JSONObject json) {
        return convertTable(date, json, "Gesamt");
    }

    static List<Integer> convertTestungenSchule(JSONObject json) {
        return convertTestungen(json, "Anzahl durchgeführter Testungen in der Schule ");
    }

    static List<Integer> convertTestungen(JSONObject json, String verificationString) {
        if (verificationString != null) {
            if (!json.getString("header").startsWith(verificationString)) {
                throw new RuntimeException("ungültiger header: " + json.getString("header"));
            }
        }
        JSONArray data = json.getJSONArray("data");
        return Arrays.asList(numberAtPosition(data, 0),
                numberAtPosition(data, 1),
                numberAtPosition(data, 2));
    }

    static List<Integer> convertTestungenArt(JSONObject json) {
        return convertTestungen(json, null);
    }

    static List<Integer> convertTestungenPositiv(JSONObject json) {
        return convertTestungen(json, "Testungen in der Schule mit einem positiven Testergebnis");
    }

    static int numberAtPosition(JSONArray data, int position) {
        return NumberUtils.toInt(data.getJSONObject(position).getString("number").replaceFirst("\\.", ""));
    }

    private void collectExistingWeekyData(String code, File targetFile) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        PrintStream out = new PrintStream(targetFile);
        for (String week : AVAILABLE_WEEKS) {
            HttpGet get = new HttpGet("https://api.public.polyteia.de/data/" + code + "/?week=" + week);
            get.addHeader("Authorization", "Id bimi_sh_public");
            CloseableHttpResponse response = client.execute(get);
            if (response.getStatusLine().getStatusCode() == 200) {
                JSONObject json = new JSONObject(new JSONTokener(response.getEntity().getContent()));
                List<Integer> row = convertTestungen(json, null);
                out.print(week + "," + row.stream().map(String::valueOf).collect(Collectors.joining(",")));
                out.println();
            }
            response.close();
        }
        out.close();
    }

    private void collectExistingDailyData(String code, File targetFile) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        PrintStream out = new PrintStream(targetFile);
        for (String date : AVAILABLE_DAYS) {
            HttpGet get = new HttpGet("https://api.public.polyteia.de/data/" + code + "/?schultag=" + date);
            get.addHeader("Authorization", "Id bimi_sh_public");
            CloseableHttpResponse response = client.execute(get);
            if (response.getStatusLine().getStatusCode() == 200) {
                JSONObject json = new JSONObject(new JSONTokener(response.getEntity().getContent()));
                List<List<String>> table = convertFaelleNachSchulart(date, json);
                for (List<String> row : table) {
                    out.println(String.join(",", row));
                }
            }
            response.close();
        }
        out.close();
    }

    private JSONObject downloadRawData(String url) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        final HttpGet get = new HttpGet(url);
        get.addHeader("Authorization", "Id bimi_sh_public");
        final CloseableHttpResponse response = client.execute(get);
        final JSONObject json;
        if (response.getStatusLine().getStatusCode() == 200) {
            json = new JSONObject(new JSONTokener(response.getEntity().getContent()));
        } else {
            log.error("Could not download {}: {}", url, response.getStatusLine());
            json = null;
        }
        response.close();
        return json;
    }


    /**
     * Liest aus der Datei mit den bisherigen Zahlen das letzte Datum.
     */
    String readLastDateFromTargetFile() throws IOException {
        String max = null;
        if (targetFile != null && targetFile.exists()) {
            final BufferedReader in = new BufferedReader(new FileReader(targetFile));
            in.readLine(); // skip header
            String line = in.readLine();
            while (line != null) {

                String date = StringUtils.substringBefore(line, ",");
                if (StringUtils.compare(date, max, true) > 0) {
                    max = date;
                }
                line = in.readLine();
            }
        }

        if (max == null) {
            if ("daily".equals(frequency)) {
                return EARLIEST_DATE;
            } else {
                return EARLIEST_WEEK;
            }
        } else {
            return max;
        }
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        if ("daily".equals(frequency)) {
            return generateDistributionForDailyDatasets(directory);
        } else if ("weekly".equals(frequency)) {
            return generateDistributionForWeeklyDatasets(directory);
        } else {
            log.warn("Unknown frequency {}.", frequency);
            return false;
        }
    }

    private boolean generateDistributionForWeeklyDatasets(File directory) throws IOException {
        String lastDate = readLastDateFromTargetFile();

        int year = Calendar.getInstance().get(Calendar.YEAR);
        int week = Calendar.getInstance().get(Calendar.WEEK_OF_YEAR);
        String weekString = year + "-W" + String.format("%02d", week);

        final PrintStream out = new PrintStream(new FileOutputStream(new File(directory, "data.csv")));

        // copy header
        BufferedReader existingData = null;
        if (targetFile != null && targetFile.exists()) {
            existingData = new BufferedReader(new FileReader(targetFile));
            out.println(existingData.readLine());
        }

        while (!weekString.equals(lastDate)) {

            List<Integer> data;
            if (CODE_TEST_ART.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?week=" + weekString);
                data = convertTestungenArt(json);
            } else if (CODE_TEST_IN_SCHULE.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?week=" + weekString);
                data = convertTestungenSchule(json);
            } else if (CODE_TEST_POSITIV.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?week=" + weekString);
                data = convertTestungenPositiv(json);
            } else {
                log.warn("Unknown code {}", code);
                data = null;
            }

            if (data != null) {
                out.println(weekString + "," + data.stream().map(String::valueOf).collect(Collectors.joining(",")));
            }

            // go to previous week
            week--;
            if (week < 1) {
                week = 51;
                year--;
            }
            weekString = year + "-W" + String.format("%02d", week);
        }

        if (existingData != null) {
            String line = existingData.readLine();
            while (line != null) {
                out.println(line);
                line = existingData.readLine();
            }
            existingData.close();
        }

        out.close();
        return true;
    }

    private boolean generateDistributionForDailyDatasets(File directory) throws IOException {

        String lastDate = readLastDateFromTargetFile();

        LocalDate dateObject = LocalDate.now().minusDays(1);

        // Copy header
        final PrintStream out = new PrintStream(new FileOutputStream(new File(directory, "data.csv")));
        BufferedReader existingData = null;
        if (targetFile != null && targetFile.exists()) {
            existingData = new BufferedReader(new FileReader(targetFile));
            out.println(existingData.readLine());
        }

        String dateString = DateTimeFormatter.ISO_DATE.format(dateObject);

        while (!dateString.equals(lastDate)) {

            final List<List<String>> table;
            if (CODE_SCHULE_GESCHLOSSEN.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?schultag=" + dateString);
                table = convertTable(dateString, json, "Gesamt");
            } else if (CODE_SCHULE_BEINTRAECHTIGT.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?schultag=" + dateString);
                table = convertTable(dateString, json, "Gesamt");
            } else if (CODE_FAELLE_SCHULART.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?schultag=" + dateString);
                table = convertTable(dateString, json, "Neue Fälle insgesamt");
            } else if (CODE_FAELLE_KREIS.equals(code)) {
                final JSONObject json = downloadRawData("https://api.public.polyteia.de/data/" + code + "/?schultag=" + dateString);
                table = convertTable(dateString, json, "Neue Fälle insgesamt");
            } else {
                log.warn("Unknown code {}", code);
                table = null;
            }

            if (table != null) {
                for (List<String> row : table) {
                    out.println(String.join(",", row));
                }
            }

            dateObject = getPreviousDate(dateObject);
            dateString = DateTimeFormatter.ISO_DATE.format(dateObject);
        }

        //copy existing data
        if (existingData != null) {
            String line = existingData.readLine();
            while (line != null) {
                out.println(line);
                line = existingData.readLine();
            }
            existingData.close();
        }
        out.close();
        return true;
    }

    LocalDate getPreviousDate(LocalDate date) {
        if (DayOfWeek.MONDAY.equals(date.getDayOfWeek())) {
            return date.minusDays(3);
        } else if (DayOfWeek.SATURDAY.equals(date.getDayOfWeek())) {
            return date.minusDays(2);
        } else {
            return date.minusDays(1);
        }
    }

}
