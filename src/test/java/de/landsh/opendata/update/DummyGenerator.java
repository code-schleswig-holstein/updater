package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;

import java.io.File;

public class DummyGenerator implements Generator {

    public static int invokationCounter = 0;

    public DummyGenerator(DatasetUpdate update) {

    }

    @Override
    public boolean generateDistributions(File directory) {
        invokationCounter++;
        return false;
    }
}
