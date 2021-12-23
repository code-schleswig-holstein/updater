package de.landsh.opendata.coronardeck;

import java.io.PrintStream;

public class CoronaWalker extends CoronaDataBaseListener{

    private final PrintStream out;

    public CoronaWalker(PrintStream out) {
        super();
        this.out = out;
    }

    @Override
    public void enterData(CoronaDataParser.DataContext ctx) {
        out.println("{");
        boolean firstEntry = true;

        for (CoronaDataParser.EntryContext entry : ctx.entry()) {
            if( firstEntry) {
                firstEntry = false;
            } else {
                out.println(",");
            }
            out.print("\""+entry.ARS()+"\": {");

            boolean firstPair = true;
            for (CoronaDataParser.PairContext pair : entry.pair()) {
                if( firstPair) {
                    firstPair = false;
                } else {
                    out.print(",");
                }
                out.print("\""+pair.NAME()+"\" : ");
                out.print(pair.VALUE());
            }

            out.print("}");

        }
        out.println(" }");
    }
}
