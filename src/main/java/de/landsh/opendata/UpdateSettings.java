package de.landsh.opendata;

import lombok.Data;

import java.util.List;

@Data
public class UpdateSettings {
    String apiKey;
    String ckanURL;
    String localDirectory;
    List<DatasetUpdate> datasets;
    String dataDirectory;
    boolean dryRun;
}
