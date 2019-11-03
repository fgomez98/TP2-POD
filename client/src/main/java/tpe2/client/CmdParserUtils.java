package tpe2.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import java.io.IOException;

class CmdParserUtils {

    static void  init(final String[] args, Object o) throws IOException {
        final CmdLineParser parser = new CmdLineParser(o);
        if (args.length < 1) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    static void checkUsage(Client client) {
        final CmdLineParser parser = new CmdLineParser(client);
        if ((client.getQueryNum() == 2 || client.getQueryNum() == 5 || client.getQueryNum() == 4) && client.getN() == null) {
            System.err.println("Missing param -Dn for query");
            parser.printUsage(System.err);
            System.exit(1);
        } else if (client.getQueryNum() == 4 && client.getOriginOaci() == null) {
            System.err.println("Missing param -Doaci for query");
            parser.printUsage(System.err);
            System.exit(1);
        } else if (client.getQueryNum() == 6 && client.getDmin() == null) {
            System.err.println("Missing param -Dmin for query");
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

}
