package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;

import java.io.File;
import java.io.FileWriter;
import java.util.UUID;

/**
 * Dieser Generator wird im Zielverzeichnis immer eine neue CSV-Datei mit dem Namen der id plus Suffix csv und
 * zufälligem Inhalt erzeugen.
 */
public class WriteNewFileGenerator implements Generator {
    private String id;

    public WriteNewFileGenerator(String id, DatasetUpdate update) {
        this.id = id;
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        final File targetFile = new File(directory, id + ".csv");
        final FileWriter writer = new FileWriter(targetFile);
        writer.write(UUID.randomUUID().toString());
        writer.close();

        return true;
    }
}
