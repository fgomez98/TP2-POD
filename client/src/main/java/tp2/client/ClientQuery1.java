package tp2.client;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import org.slf4j.LoggerFactory;
import query1.Query1;
import tpe2.api.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Flight;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class ClientQuery1 {

    private List<String> ips;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports", required = true)
    private void setIps(String s) throws CmdLineException {
        List<String> list = Arrays.asList(s.split(";"));
        for (String ip : list) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = list;
    }


    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String din;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String dout;

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    public String getDout() {
        return dout;
    }

    public static void main(String[] args) {
        ClientQuery1 query = new ClientQuery1();
        LoggerFactory.getLogger(org.apache.commons.beanutils.converters.AbstractConverter.class);
        Logger logger = createLoggerFor("Client", query.getDout()+"/query1.txt");
        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        for (String ip : query.getIps()) {
            System.out.println(ip);
        }

        try {
            HazelcastInstance hz = Hazelcast.newHazelcastInstance();

            logger.info("Inicio de la lectura del archivo");
            List<Airport> airports = CSVUtils.CSVReadAirports("/Users/pilo/development/itba/pod/TP2-POD/server/src/main/resources/aeropuertos.csv");
            List<Flight> flights = CSVUtils.CSVReadFlights("/Users/pilo/development/itba/pod/TP2-POD/server/src/main/resources/movimientos.csv");
            logger.info("Fin de lectura del archivo");

            logger.info("Inicio del trabajo map/reduce");
            System.out.println("OACI;Denominación;Movimientos");
            new Query1().movPerAirPorts(hz, airports, flights).forEach(System.out::println);
            logger.info("Fin del trabajo map/reduce");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Logger createLoggerFor(String string, String file) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();

        ple.setPattern("%d{yyyy-MM-dd HH:mm:ss:xxxx} %-5p %c{1}:%L - %m%n");
        ple.setContext(lc);
        ple.start();
        FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
        fileAppender.setFile(file);
        fileAppender.setEncoder(ple);
        fileAppender.setContext(lc);
        fileAppender.start();

        Logger logger = (Logger) LoggerFactory.getLogger(string);
        logger.addAppender(fileAppender);
        logger.setLevel(Level.INFO);
        logger.setAdditive(false); /* set to true if root should log too */

        return logger;
    }
}
