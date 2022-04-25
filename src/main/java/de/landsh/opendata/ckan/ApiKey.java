package de.landsh.opendata.ckan;

public class ApiKey {
    public static final ApiKey ANONYMOUS = new ApiKey(null);

    private String key;

    public ApiKey(String key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return key;
    }
}
