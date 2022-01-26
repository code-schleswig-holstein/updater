package de.landsh.opendata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.landsh.opendata.ckan.ApiKey;
import de.landsh.opendata.ckan.CkanAPI;
import de.landsh.opendata.ckan.Resource;
import de.landsh.opendata.update.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Bearbeiten von Datenänderungen direkt über das CKAN API.
 */
public class OpenDataUpdatesCkan {
    private static final Logger log = LoggerFactory.getLogger(OpenDataUpdatesCkan.class);
    private CkanAPI ckanAPI;
    private File localDataDir;
    private boolean dryRun = false;

    public OpenDataUpdatesCkan(String baseURL, ApiKey apiKey) {
        ckanAPI = new CkanAPI(baseURL, apiKey);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1 || !new File(args[0]).exists()) {
            System.out.println("USAGE: java OpenDataUpdatesCkan config.yml [single_dataset_id]");
            System.exit(2);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        UpdateSettings settings = mapper.readValue(new File(args[0]), UpdateSettings.class);

        final ApiKey apiKey = new ApiKey(settings.isDryRun() ? "DRY_RUN" : settings.getApiKey());
        final OpenDataUpdatesCkan self = new OpenDataUpdatesCkan(settings.getCkanURL(), apiKey);
        self.localDataDir = new File(settings.localDirectory);
        self.dryRun = settings.isDryRun();

        if (self.dryRun) {
            log.info("Probelauf aktiv, es werden keine Änderungen am Open-Data-Portal vorgenommen.");
        }

        if (args.length > 1) {
            // nur ein Datensatz
            final String singleDatasetId = args[1];
            for (DatasetUpdate u : settings.getDatasets()) {
                if (singleDatasetId.equals(u.getDatasetId()) || singleDatasetId.equals(u.getCollectionId())) {
                    self.work(u);
                }
            }
        } else {
            for (DatasetUpdate u : settings.getDatasets()) {
                if (u.isActive()) {
                    self.work(u);
                }
            }
        }

    }

    public static String getExtrasValue(JSONObject dataset, String key) {
        final JSONArray extras = dataset.getJSONArray("extras");
        for (Object o : extras) {
            final JSONObject extra = (JSONObject) o;
            if (key.equals(extra.getString("key"))) {
                return extra.getString("value");
            }
        }
        return null;
    }

    static String getMimeType(String format) {
        final String result;

        if ("PDF".equals(format)) {
            result = "application/pdf";
        } else if ("CSV".equals(format)) {
            result = "text/csv";
        } else if ("JSON".equals(format)) {
            result = "application/json";
        } else if ("XML".equals(format)) {
            result = "application/xml";
        } else {
            result = "application/octet-stream";
        }

        return result;
    }

    void setLocalDataDir(File file) {
        this.localDataDir = file;
    }

    void setCkanAPI(CkanAPI ckanAPI) {
        this.ckanAPI = ckanAPI;
    }

    /**
     * Datei ist öffentlich und es werden nur Zeilen hinzugefügt.
     * <p>
     * Als einziges muss das Änderungsdatum aktualisiert werden.
     * <p>
     * Es gibt nur ein Dataset. Die Distribution zeigt auf die echte Datei, die bereits überschrieben wurde.
     * Daher taugt sie nicht zum Vergleich, ob es Änderungen gab. Es muss mit der letzten lokalen Kopie
     * verglichen werden.
     */
    boolean appendPublic(DatasetUpdate update, File localDataDir) throws IOException {

        if (StringUtils.isBlank(update.datasetId)) {
            log.error("Die datasetId muss angegeben werden.");
            return false;
        }

        final JSONObject dataset = ckanAPI.readDataset(update.datasetId);

        if (dataset == null) {
            log.error("Datensatz {} gibt es nicht.", update.datasetId);
            return false;
        }

        final String timeNow = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        setExtraValue(dataset, "modified", timeNow);

        // Gültigkeitszeitraum bestimmen und ändern
        if (StringUtils.isNotEmpty(getExtrasValue(dataset, "temporal_end"))) {
            setExtraValue(dataset, "temporal_end", timeNow);
        }

        // Geändertes Dataset schreiben
        if (!dryRun) {
            ckanAPI.updatePackage(dataset);
        }
        log.info("Dataset {} akualisiert.", dataset.getString("id"));
        return true;
    }

    /**
     * Es werden nur Zeilen hinzugefügt, aber die Datei ist nicht öffentlicht.
     * <p>
     * Es gibt nur ein Dataset. Die Distribution an diesem Dataset muss ausgetauscht werden und das Änderungsdatum
     * muss aktualisiert werden.
     */
    boolean appendPrivate(DatasetUpdate update, File localDataDir) throws IOException, NoSuchAlgorithmException {

        if (StringUtils.isBlank(update.datasetId)) {
            log.error("Die datasetId muss angegeben werden.");
            return false;
        }

        final JSONObject dataset = ckanAPI.readDataset(update.datasetId);

        if (dataset == null) {
            log.error("Datensatz {} gibt es nicht.", update.datasetId);
            return false;
        }

        final String timeNow = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        setExtraValue(dataset, "modified", timeNow);

        // Gültigkeitszeitraum bestimmen und ändern
        if (StringUtils.isNotEmpty(getExtrasValue(dataset, "temporal_end"))) {
            setExtraValue(dataset, "temporal_end", timeNow);
        }

        // Distributionen austauschen
        final Set<String> oldResourceIds = ckanAPI.getResources(dataset).stream().map(Resource::getId).collect(Collectors.toSet());
        // neue Distributionen hinzufügen
        final File[] files = localDataDir.listFiles();
        if (files != null) {
            for (File file : files) {
                final String format = StringUtils.upperCase(StringUtils.substringAfterLast(file.getName(), "."));
                if (!dryRun) {
                    if (ckanAPI.uploadFile(update.datasetId, file, file.getName(), format, getMimeType(format))) {
                        log.info("Datei {} erfolgreich hochgeladen.", file.getName());
                    } else {
                        log.warn("Hochladen fehlgeschlagen für Datei {}", file.getName());
                    }
                }
            }
        }
        // alte Distributionen entfernen
        for (String id : oldResourceIds) {
            if (!dryRun) {
                ckanAPI.deleteResource(id);
            }
        }

        // Geändertes Dataset schreiben
        //  ckanAPI.updatePackage(dataset);
        log.info("Dataset {} akualisiert.", dataset.getString("id"));
        return true;
    }

    /**
     * Neuste Datei ist öffentlich, wird aber einfach überschrieben.
     * <p>
     * Es gibt eine Collection "abc", die als neustes Dataset eines mit dem Namen "abc-aktuell" enthält.
     * Die Distribution zeigt auf die echte Datei, die bereits überschrieben wurde. Daher taugt sie nicht zum
     * Vergleich, ob es Änderungen gab. Es muss mit der letzten lokalen Kopie verglichen werden.
     */
    boolean overwritePublic(DatasetUpdate update, File localDataDir) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isBlank(update.collectionId)) {
            log.error("Die collectionId muss angegeben werden.");
            return false;
        }

        final String newestDatasetId = ckanAPI.findNewestDataset(update.collectionId);

        if (newestDatasetId == null) {
            log.error("In der Collection {} gibt es keinen Datensatz.", update.collectionId);
            return false;
        }

        final JSONObject existingDataset = ckanAPI.readDataset(newestDatasetId);
        log.info("Änderung erkannt an Dataset " + newestDatasetId);
        final String timeNow = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        final JSONObject newDataset = new JSONObject(existingDataset.toString());
        newDataset.remove("resources");
        newDataset.remove("id");
        newDataset.put("is_new", true);

        setExtraValue(existingDataset, "modified", timeNow);
        setExtraValue(existingDataset, "temporal_start", timeNow);
        setExtraValue(existingDataset, "issued", timeNow); // damit der Datensatz in der Collection der neuste ist.

        setExtraValue(newDataset, "modified", timeNow);
        setExtraValue(newDataset, "issued", timeNow);
        setExtraValue(newDataset, "temporal_end", timeNow);
        setExtraValue(newDataset, "identifier", UUID.randomUUID().toString());

        final String realTitle = newDataset.getString("title");
        final String titleWithDate = realTitle + ' ' + LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        newDataset.put("title", titleWithDate);

        // Write new dataset with temporary title to generate a nice dataset id.

        final String newDatasetId;

        if (!dryRun) {
            newDatasetId = ckanAPI.createPackage(newDataset);
            log.info("Neues Dataset mit der Id {} angelegt.", newDatasetId);

            // Change the title of the newly uploaded dataset to the real title
            ckanAPI.changeTitle(newDatasetId, realTitle);
        } else {
            log.info("Es würde ein neues Dataset angelegt.");
            log.info("  title = {}", newDataset.getString("title"));
            log.info("  issued = {}", getExtrasValue(newDataset, "issued"));
            log.info("  temporal_start = {}", getExtrasValue(newDataset, "temporal_start"));
            log.info("  temporal_end = {}", getExtrasValue(newDataset, "temporal_end"));
            newDatasetId = "DRY_DRUN";
        }

        final File[] files = localDataDir.listFiles();
        if (files != null) {
            for (File file : files) {
                final String format = StringUtils.upperCase(StringUtils.substringAfterLast(file.getName(), "."));
                if (!dryRun) {
                    if (ckanAPI.uploadFile(newDatasetId, file, file.getName(), format, getMimeType(format))) {
                        log.info("Datei {} erfolgreich hochgeladen.", file.getName());
                    } else {
                        log.warn("Hochladen fehlgeschlagen für Datei {}", file.getName());
                    }
                } else {
                    log.info("Datei {} mit Format {} und MIME-Type {} würde hochgeladen.", file.getName(), format, getMimeType(format));
                }
            }
        }

        if (!dryRun) {
            ckanAPI.putDatasetInCollection(newDatasetId, update.collectionId);
            ckanAPI.updatePackage(existingDataset);
        } else {
            log.info("Dataset würde in Collection {} eingeordnet", update.collectionId);
            log.info("Existiernedes Dataset würde aktualisiert:");
            log.info("  temporal_start = {}", getExtrasValue(existingDataset, "temporal_start"));
            log.info("  temporal_end = {}", getExtrasValue(existingDataset, "temporal_end"));
        }

        return true;

    }

    /**
     * Datei wird einfach überschrieben und ist nicht öffentlich erreichbar.
     * <p>
     * Es gibt eine Collection "abc", die einen neusten Datensatz z.B. "abc-2021-03-01" enthält. Dieser enthält
     * bereits eine Distribution mit einer Kopie der Datei und Prüfsumme. Die vom geheimen Platz geladenen Datei kann
     * gegen diese Prüfsumme geprüft werden.
     */
    boolean overwritePrivate(DatasetUpdate update, File localDataDir) throws IOException, NoSuchAlgorithmException {
        if (StringUtils.isBlank(update.collectionId)) {
            log.error("Die collectionId muss angegeben werden.");
            return false;
        }

        final String newestDatasetId = ckanAPI.findNewestDataset(update.collectionId);

        if (newestDatasetId == null) {
            log.error("In der Collection {} gibt es keinen Datensatz.", update.collectionId);
            return false;
        }

        final JSONObject existingDataset = ckanAPI.readDataset(newestDatasetId);
        log.info("Änderung erkannt an Dataset " + newestDatasetId);
        final String timeNow = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

        final JSONObject newDataset = new JSONObject(existingDataset.toString());
        newDataset.remove("resources");
        newDataset.remove("id");
        newDataset.put("is_new", true);

        setExtraValue(existingDataset, "temporal_end", timeNow);

        setExtraValue(newDataset, "modified", timeNow);
        setExtraValue(newDataset, "issued", timeNow);
        setExtraValue(newDataset, "temporal_start", timeNow);
        setExtraValue(newDataset, "identifier", UUID.randomUUID().toString());

        final String realTitle = newDataset.getString("title");
        final String titleWithDate = realTitle + ' ' + LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        newDataset.put("title", titleWithDate);

        // Write new dataset with temporary title to generate a nice dataset id.
        final String newDatasetId;

        if (!dryRun) {
            newDatasetId = ckanAPI.createPackage(newDataset);
            log.info("Neues Dataset mit der Id {} angelegt.", newDatasetId);

            // Change the title of the newly uploaded dataset to the real title
            ckanAPI.changeTitle(newDatasetId, realTitle);
        } else {
            newDatasetId = "DRY_RUN";
        }

        final File[] files = localDataDir.listFiles();
        if (files != null) {
            for (File file : files) {
                final String format = StringUtils.upperCase(StringUtils.substringAfterLast(file.getName(), "."));
                if (!dryRun) {
                    if (ckanAPI.uploadFile(newDatasetId, file, file.getName(), format, getMimeType(format))) {
                        log.info("Datei {} erfolgreich hochgeladen.", file.getName());
                    } else {
                        log.warn("Hochladen fehlgeschlagen für Datei {}", file.getName());
                    }
                }
            }
        }

        if (!dryRun) {
            ckanAPI.updatePackage(existingDataset);

            ckanAPI.putDatasetInCollection(newDatasetId, update.collectionId);
        }

        return true;

    }

    /**
     * Prüfe, ob die zwei Verzeichnisse identischen Inhalt haben.
     */
    public boolean directoriesAreEqual(File dir1, File dir2) throws IOException {
        File[] files1 = dir1.listFiles();
        File[] files2 = dir2.listFiles();

        if (files1 == null || files2 == null) return false;

        Arrays.sort(files1, Comparator.comparing(File::getName));
        Arrays.sort(files2, Comparator.comparing(File::getName));
        if (files1.length != files2.length) return false;

        for (int i = 0; i < files1.length; i++) {
            if (!filesAreEqual(files1[i], files2[i])) {
                return false;
            }
        }
        return true;
    }

    private boolean filesAreEqual(java.io.File a, java.io.File b) throws IOException {
        if (a.isFile() && b.isFile()) return
                StringUtils.equals(
                        DigestUtils.md5Hex(FileUtils.readFileToByteArray(a)),
                        DigestUtils.md5Hex(FileUtils.readFileToByteArray(b))
                );
        return false;
    }

    boolean work(DatasetUpdate update) throws Exception {

        if (!update.isActive())
            return false;

        // Prüfen, ob der Lauf zum Zeitplan passt
        if (update.dayOfMonth != null && LocalDate.now().getDayOfMonth() != update.dayOfMonth) {
            return false;
        }
        if (update.dayOfWeek != null && LocalDate.now().getDayOfWeek().getValue() != update.dayOfWeek) {
            return false;
        }

        final String id = update.collectionId != null ? update.collectionId : update.datasetId;

        final Generator generator;
        if (JustDownloadGenerator.class.getCanonicalName().equals(update.generator)
                || "just-download".equals(update.generator)) {
            generator = new JustDownloadGenerator(id + "." + update.format, update);
        } else if (WappenrolleGenerator.class.getCanonicalName().equals(update.generator)
                || "de.landsh.opendata.processor.Wappenrolle".equals(update.generator)) {
            generator = new WappenrolleGenerator(update);
        } else if (DenkmallisteGenerator.class.getCanonicalName().equals(update.generator)
                || "de.landsh.opendata.processor.Denkmalliste".equals(update.generator)) {
            generator = new DenkmallisteGenerator(update);
        } else if (CsvSortLines.class.getCanonicalName().equals(update.generator)) {
            generator = new CsvSortLines(id + ".csv", update);
        } else {
            generator = findGeneratorDynamically(update.generator, id, update);
            if (generator == null) {
                log.warn("Unbekannter Generator {} für Datensatz {}.", update.generator, id);
                return false;
            }
        }

        final Path path = Paths.get(FileUtils.getTempDirectory().getAbsolutePath(), UUID.randomUUID().toString());
        final File tmpdir = Files.createDirectories(path).toFile();
        tmpdir.deleteOnExit();

        try {
            generator.generateDistributions(tmpdir);
        } catch (Exception ex) {
            log.error("Could not generate distributions: {}", ex.getMessage());
            return false;
        }

        File localCopyDir = new File(localDataDir, id);

        boolean success;
        if (localCopyDir.isDirectory()) {
            if (!directoriesAreEqual(tmpdir, localCopyDir)) {
                log.info("Unterschiede bei {} erkannt.", id);

                // Die frisch heruntergeladenen Daten liegen in tmpdir.

                if (update.type == DatasetUpdate.Type.APPEND) {
                    if (update.isPrivate) {
                        success = appendPrivate(update, tmpdir);
                    } else {
                        success = appendPublic(update, tmpdir);
                    }
                } else if (update.type == DatasetUpdate.Type.OVERWRITE) {
                    if (update.isPrivate) {
                        success = overwritePrivate(update, tmpdir);
                    } else {
                        success = overwritePublic(update, tmpdir);
                    }
                } else {
                    log.error("Unbekannter Modus {}", update.type);
                    success = false;
                }
            } else {
                // keine Änderung an den Distributionen
                success = true;
            }
        } else {
            if (!localCopyDir.mkdirs()) {
                log.error("Konnte Verzeichnis mit lokalen Kopien {} nicht erzeugen.", localCopyDir);
            }

            log.warn("Verzeichnis mit lokalen Kopien für Datensatz {} existiert nicht.", id);
            success = false;
        }

        if (!dryRun) {
            // lokale Kopie aktualisieren
            final File[] files = tmpdir.listFiles();

            if (files == null) {
                log.warn("Keine Dateien in Verzeichnis {}", tmpdir);
            } else {

                for (File file : files) {
                    FileUtils.copyFileToDirectory(file, localCopyDir);
                }
            }
        }

        // temporäres Verzeichnis wieder entfernen
        FileUtils.deleteDirectory(tmpdir);

        return success;
    }

    Generator findGeneratorDynamically(String generator, String id, DatasetUpdate updateSettings) throws InvocationTargetException, InstantiationException, IllegalAccessException {

        final Class<?> clazz;
        try {
            clazz = Class.forName(generator);
        } catch (ClassNotFoundException exception) {
            log.error("Es gibt keinen Generator {} für Datensatz {}.", generator, id);
            return null;
        }

        // try two types of constructor
        try {
            final Constructor<?> constructor = clazz.getConstructor(String.class, DatasetUpdate.class);
            return (Generator) constructor.newInstance(id, updateSettings);
        } catch (NoSuchMethodException ignore) {
        }

        try {
            final Constructor<?> constructor = clazz.getConstructor();
            return (Generator) constructor.newInstance();
        } catch (NoSuchMethodException ignore) {
        }

        log.error("Es gibt keinen passenden Constructor im Generator {} für Datensatz {}.", generator, id);
        return null;
    }

    void setExtraValue(JSONObject dataset, String key, String value) {
        final JSONObject entry = new JSONObject();
        entry.put("key", key);
        entry.put("value", value);

        int foundAtPosition = -1;
        JSONArray extras = dataset.getJSONArray("extras");
        for (int i = 0; i < extras.length(); i++) {
            JSONObject it = (JSONObject) extras.get(i);
            if (key.equals(it.getString("key"))) {
                foundAtPosition = i;
            }
        }
        if (foundAtPosition > -1) {
            extras.put(foundAtPosition, entry);
        } else {
            // Es gibt noch keinen entsprechenden Eintrag in "extras"
            extras.put(entry);
        }
    }

}

