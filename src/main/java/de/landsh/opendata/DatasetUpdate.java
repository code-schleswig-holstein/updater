package de.landsh.opendata;

import lombok.Data;

import java.io.File;
import java.util.List;
import java.util.Map;

@Data
public class DatasetUpdate {

    String collectionId;
    Type type;
    String datasetId;
    boolean active = true;
    boolean isPrivate = false;
    File localFile;
    String originalURL;
    boolean localCopy = false;
    List<String> additionalResources;
    String username;
    String password;
    String generator;
    Map<String, String> generatorArgs;
    Integer dayOfMonth;
    Integer dayOfWeek;
    String format = "csv";

    public enum Type {
        APPEND, OVERWRITE, METADATA
    }
}
