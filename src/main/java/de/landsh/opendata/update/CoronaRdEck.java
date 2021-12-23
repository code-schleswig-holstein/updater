package de.landsh.opendata.update;


import de.landsh.opendata.DatasetUpdate;
import de.landsh.opendata.coronardeck.CoronaDataLexer;
import de.landsh.opendata.coronardeck.CoronaDataParser;
import de.landsh.opendata.coronardeck.CoronaWalker;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;

/**
 * Corona-Dashboard des Kreises Rendsburg-Eckernförde
 */
public class CoronaRdEck implements Generator {

    private final String url;

    public CoronaRdEck(String url) {
        this.url = url;
    }

    public CoronaRdEck(String id, DatasetUpdate update) {
        this.url = update.getOriginalURL();
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        final File file = new File(directory, "daten.json");
        final InputStream in = new URL(url).openStream();
        final ANTLRInputStream input = new ANTLRInputStream(in);
        final CoronaDataLexer lexer = new CoronaDataLexer(input);
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final CoronaDataParser parser = new CoronaDataParser(tokens);
        final ParseTree tree = parser.data();
        final ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new CoronaWalker(new PrintStream(new FileOutputStream(file))), tree);
        return true;
    }
}
