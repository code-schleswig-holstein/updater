package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.BooleanUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Lädt eine CSV-Datei herunter und sortiert die Zeilen.
 */
public class CsvSortLines implements Generator {

    private final JustDownloadGenerator downloadGenerator;
    private final String distributionName;

    private boolean containsHeaderLine = true;

    public CsvSortLines(String distributionName, DatasetUpdate update) {
        this.downloadGenerator = new JustDownloadGenerator("temp", update);
        this.distributionName = distributionName;

        this.containsHeaderLine = !BooleanUtils.toBoolean(update.getGeneratorArgs().get("no_header"));

    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        downloadGenerator.generateDistributions(directory);

        File inputFile = new File(directory, "temp");

        File targetFile = new File(directory, distributionName);
        PrintWriter out = new PrintWriter(new FileWriter(targetFile));

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        if (containsHeaderLine) {
            out.println(in.readLine());
        }
        List<String> lines = new ArrayList<>();
        String line = in.readLine();
        while (line != null) {
            lines.add(line);
            line = in.readLine();
        }
        lines.sort(String::compareTo);

        for (String s : lines) {
            out.println(s);
        }
        out.close();

        inputFile.delete();

        return true;
    }
}
