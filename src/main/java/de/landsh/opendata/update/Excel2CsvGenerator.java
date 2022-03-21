package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

public class Excel2CsvGenerator implements Generator {
    private static final String DELIMITER = ",";
    private static final Logger log = LoggerFactory.getLogger(Excel2CsvGenerator.class);
    private final JustDownloadGenerator downloadGenerator;
    private final String distributionName;
    private final int sheetNumber;

    public Excel2CsvGenerator(String distributionName, DatasetUpdate update) {
        this.downloadGenerator = new JustDownloadGenerator("temp.xlsx", update);
        this.distributionName = distributionName;
        if ( update.getGeneratorArgs() != null && update.getGeneratorArgs().size() > 0) {
            this.sheetNumber = NumberUtils.toInt(update.getGeneratorArgs().get("sheet"));
        } else {
            this.sheetNumber = 0;
        }
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {

        if (!downloadGenerator.generateDistributions(directory)) {
            return false;
        }

        final File inputFile = new File(directory, "temp.xlsx");
        final FileWriter out = new FileWriter(new File(directory, distributionName + ".csv"));

        final Workbook wb = WorkbookFactory.create(inputFile);

        final DataFormatter dataFormatter = new DataFormatter();

        final Sheet sheet = wb.getSheetAt(sheetNumber);
        final Iterator<Row> it = sheet.rowIterator();
        while (it.hasNext()) {
            final Row row = it.next();
            final Iterator<Cell> it2 = row.cellIterator();
            while (it2.hasNext()) {
                final Cell cell = it2.next();
                final String value = dataFormatter.formatCellValue(cell).replaceAll("\n", " ").trim();
                if (value.contains(DELIMITER)) {
                    out.write("\"");
                    out.write(value);
                    out.write("\"");
                } else {
                    out.write(value);
                }
                if (it2.hasNext()) {
                    out.write(DELIMITER);
                }
            }
            out.write("\n");
        }

        out.close();

        if (!inputFile.delete()) {
            log.warn("Could not delete file {}", inputFile);
        }

        return true;
    }
}
