package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;

import java.io.File;

public class DummyGenerator implements Generator {

    public DummyGenerator(String id, DatasetUpdate update) {

    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        return false;
    }
}
