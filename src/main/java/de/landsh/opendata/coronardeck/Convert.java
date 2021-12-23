package de.landsh.opendata.coronardeck;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Convert {
    public static void main(String[] args) throws IOException {
        final  ANTLRInputStream input = new ANTLRInputStream( new FileReader("/tmp/covid19dashboardrdeck/2021-11-12_100007.json"));

        final  CoronaDataLexer lexer = new CoronaDataLexer(input);

        final CommonTokenStream tokens = new CommonTokenStream(lexer);

        final CoronaDataParser parser = new CoronaDataParser(tokens);

        final ParseTree tree = parser.data();

        final ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk( new CoronaWalker(System.out), tree );
    }
}
