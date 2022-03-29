# OpenDataUpdatesCkan

Dies ist das zentrale Harvesting-Programm, das externe Datenquellen überprüft und bei Änderungen Datensätze im Open-Data-Portal anlegt bzw. aktualisiert.

Aufruf über die Kommandozeile erfolgt so:

```bash
./mvnw -q exec:java -Dexec.mainClass=de.landsh.opendata.OpenDataUpdatesCkan -Dexec.args="config/update.yaml"
```

Einziger Parameter ist der Pfad zur Konfigurationsdatei.


## Konfigurationsdatei

Hierbei handelt es sich um eine YAML-Datei, in der Zugangsdaten, globale Parameter und alle zu bearbeitenden Datenquellen verzeichnet sind.

```yaml
ckanURL: https://opendata-stage.schleswig-holstein.de
apiKey: XXXX

localDirectory: /data/opendata/local-copies/
dryRun: false

datasets:
  - collectionId: badegewasser-stammdaten
    originalURL: http://efi2.schleswig-holstein.de/bg/opendata/v_badegewaesser_odata.csv
    type: OVERWRITE
    generator: de.landsh.opendata.update.CsvSortLines
    generatorArgs:
      no_header: true

  - datasetId: badegewasser-saisondauer
    originalURL: http://efi2.schleswig-holstein.de/bg/opendata/v_badesaison_odata.csv
    type: APPEND
    generator: just-download
```

### Pflichtparameter

- `ckanURL` -  Adresse des Open-Data-Portal, z.B. https://opendata-stage.schleswig-holstein.de
- `apiKey` -  API-Schlüssel eines CKAN-Nutzers, der berechtigt ist, für alle betroffenen Organisationen Datensätze anzulegen
- `localDirectory` -  ein lokales Verzeichnis, in dem Dateien zum Vergleichen abgelegt werden

### optionale Parameter

- `dryRun` - wenn hier `true` steht, werden keine Schreibzugriffe auf das Open-Data-Portal durchgeführt

### Datensätze

Bei Datensätze müssen zwei Arten unterschieden werden:

1. Zeitreihen mit `collectionId`, an die bei Bedarf neue Datensätze angehängt werden
2. einzelne Datensätze mit `datasetId`, die bei Bedarf aktualisiert werden

## Arten des Überschreibens

Es gibt vier Möglichkeiten, wie ein Datensatz aktualisiert wird. Dies wird durch die Angaben `type` und `private` festgelegt.

| type      | private | Beispiel                                                     |
| --------- | ------- | ------------------------------------------------------------ |
| OVERWRITE |         | https://opendata.schleswig-holstein.de/dataset/badegewasser-stammdaten-aktuell |
| OVERWRITE | true    | https://opendata.schleswig-holstein.de/collection/ln-2020-statusliste-schulen/aktuell |
| APPEND    |         | https://opendata.schleswig-holstein.de/dataset/badegewasser-messungen |
| APPEND    | true    | https://opendata.schleswig-holstein.de/dataset/corona-zahlen-kreis-stormarn |

## Generatoren

Die Generatoren sind dafür zuständig, Dateien der Datensätze zu erzeugen. Das können sie z.B. ganz einfach durch Herunterladen einer Datei machen. Möglich sind aber auch komplexere Vorgänge, wie das Befragen einer Schnittstelle und das Umformen von Daten.

- `JustDownloadGenerator` - Lädt die Datei von der mit `originalURL` angegebenen Adresse herunter.-
- `CsvSortLines` - Lädt eine CSV-Datei von der mit `originalURL` angegebenen Adresse herunter und sortiert in dieser die Zeilen ab Zeile 2 (die Kopfzeile der CSV-Datei wird also nicht mit sortiert)
- `DenkmallisteGenerator` - ein für die Denkmalliste Schleswig-Holstein spezialisierter Generator
- `WappenrolleGenerator` - ein für die Kommunale Wappenrolle Schleswig-Holstein spezialisierter Generator
- `Excel2CsvGenerator` - lädt eine Excel-Datei herunter und wandelt diese in eine CSV-Datei  um

### CsvSortLines

Argumente:

- `no_header` - die CSV-Datei enthält *keine* Kopfzeile - alle Zeilen sollen sortiert werden

### DenkmallisteGenerator

Der Generator `Denkmallist` benötigt fünf Argumente:

1. `county` - Name des Kreises
2. `json` - Adresse der JSON-Datei alle Denkmale in Schleswig-Holstein
3. `pdf` - Adresse der PDF-Datei für den Kreis
4. `photoDirecotry` - Verzeichnis mit den Fotos der Denkmale
5. `photoBaseURL` - Internetadresse, unter der die Fotos der Denkmale öffentlich zu erreichen sind

### Excel2CsvGenerator

Der `Excel2CsvGenerator` hat ein optionales Argument:

- `sheet` - gibt die Nummer des Arbeitsblattes an, das in eine CSV-Datei gewandelt werden soll. Das erste Arbeitsblatt hat die Nummer 0; in diesem Fall müsste der Parameter nicht angegeben werden.

### WappenrolleGenerator

Der Generator `Wappenrolle` benötigt ein Argument mit dem Namen `type`:

`wappen` für die Erzeugung der Datei mit den Wappen oder `flaggen` für die Erzeugung der Datei mit den Flaggen
