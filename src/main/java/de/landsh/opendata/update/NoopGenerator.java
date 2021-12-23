package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;

import java.io.File;

/**
 * Dieser Generator macht gar nichts.
 */
public class NoopGenerator implements Generator {

    public NoopGenerator(String id, DatasetUpdate update) {

    }

    public NoopGenerator() {
        
    }

    @Override
    public boolean generateDistributions(File directory)  {
        return true;
    }
}
