package de.landsh.opendata.update;

import java.io.File;

public interface Generator {


    boolean generateDistributions(File directory) throws Exception;
}
