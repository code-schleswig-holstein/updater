package de.landsh.opendata.update;

import java.io.File;

public class DummyGenerator implements Generator {

    public DummyGenerator(String id, String originalURL, String[] generatorArgs) {

    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        return false;
    }
}
