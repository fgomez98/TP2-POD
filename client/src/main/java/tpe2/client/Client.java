package tpe2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Client {

    private Integer queryNum;

    private List<String> ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String din;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are", required = true)
    private String dout;

    private Integer n;

    private String originOaci;

    private Long dmin;

    @Option(name = "-Dquery", aliases = "--queryNumber", usage = "query number to execute", required = true)
    private void setQueryNum(Integer queryNum) throws CmdLineException {
        if (queryNum < 0 || queryNum > 6) {
            throw new CmdLineException("Invalid query number, valid values 1 to 6");
        }
        this.queryNum = queryNum;
    }

    @Option(name = "-Daddresses", aliases = "--ipAddresses", usage = "one or more ip directions and ports", required = true)
    private void setIps(String s) throws CmdLineException {
        List<String> list = Arrays.asList(s.split(";"));
        for (String ip : list) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = list;
    }

    @Option(name = "-Dn", aliases = "--n", usage = "number of top airlines for queries 2,4,5", depends = {"-Dquery"})
    private void setN(Integer n) throws CmdLineException {
        if (queryNum == 2 || queryNum == 5 || queryNum == 4) {
            if (n == null) {
                throw new CmdLineException("Missing param -Dn for query");
            }
            this.n = n;
        } else {
            throw new CmdLineException("Invalid param -Dn for query");
        }
    }

    @Option(name = "-Doaci", usage = "Origin airport for query 4", depends = {"-Dquery"})
    private void setOriginOaci(String originOaci) throws CmdLineException {
        if (queryNum == 4) {
            if (originOaci == null) {
                throw new CmdLineException("Missing param -Doaci for query");
            }
            this.originOaci = originOaci;
        } else {
            throw new CmdLineException("Invalid param -Doaci for query");
        }
    }

    @Option(name = "-Dmin", aliases = "--minCount", usage = "Minimum number of shared movements to show for query 6", depends = {"-Dquery"})
    private void setDmin(Long dmin) throws CmdLineException {
        if (queryNum == 6) {
            if (dmin == null) {
                throw new CmdLineException("Missing param -Dmin for query");
            }
            this.dmin = dmin;
        } else {
            throw new CmdLineException("Invalid param -Dmin for query");
        }
    }

    public Integer getQueryNum() {
        return queryNum;
    }

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    public String getDout() {
        return dout;
    }

    public Integer getN() {
        return n;
    }

    public String getOriginOaci() {
        return originOaci;
    }

    public Long getDmin() {
        return dmin;
    }

    public static void main(String[] args) {
        Client client = new Client();

        try {
            CmdParserUtils.init(args, client);
            CmdParserUtils.checkUsage(client);
        } catch (IOException e) {
            System.out.println("There was a conflict with arguments");
            System.exit(1);
        }

        Logger logger = Helpers.createLoggerFor("Query" + client.getQueryNum(), client.getDout() + "/query" + client.getQueryNum() + ".txt");

        Query query = null;
        //instancio la query
        switch (client.getQueryNum()) {
            case 1:
                query = new Query1(client.getIps(), client.getDin(), client.getDout());
                break;
            case 2:
                query = new Query2(client.getN(), client.getIps(), client.getDin(), client.getDout());
                break;
            case 3:
                query = new Query3(client.getIps(), client.getDin(), client.getDout());
                break;
            case 4:
                query = new Query4(client.getIps(), client.getDin(), client.getDout(), client.getOriginOaci(), client.getN());
                break;
            case 5:
                query = new Query5(client.getN(), client.getIps(), client.getDin(), client.getDout());
                break;
            case 6:
                query = new Query6(client.getIps(), client.getDin(), client.getDout(), client.getDmin());
                break;
            default:
                System.err.println("Invalid query number, valid values 1 to 6");
                System.exit(1);
                break;
        }

        ClientConfig cfg = new ClientConfig();
        GroupConfig groupConfig = cfg.getGroupConfig();
        groupConfig.setName("tpe2-g8");
        groupConfig.setPassword("holamundo");
        ClientNetworkConfig clientNetworkConfig = cfg.getNetworkConfig();
        client.getIps().forEach(clientNetworkConfig::addAddress);

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);
        System.out.println("Members: " + hz.getCluster().getMembers());

        logger.info("Inicio de la lectura del archivo");

        List<Airport> airports = null;
        List<Flight> flights = null;
        try {
            /*
            Solamente utilizan los aeropuertos las queries 1, 5, 6
             */
            if (client.getQueryNum() == 1 || client.getQueryNum() == 5 || client.getQueryNum() == 6) {
                airports = CSVUtils.CSVReadAirports(client.getDout() + "/aeropuertos.csv");
            }
            flights = CSVUtils.CSVReadFlights(client.getDout() + "/movimientos.csv");
        } catch (Exception ex) {
            System.out.println("There was a conflict while reading the .csv files");
            System.exit(1);
        }

        logger.info("Fin de lectura del archivo");

        logger.info("Inicio del trabajo map/reduce");

        try {
            query.runQuery(hz, airports, flights);
        } catch (ExecutionException | InterruptedException ex) {
            System.out.println("There was a conflict while executing the query");
            System.exit(1);
        }

        logger.info("Fin del trabajo map/reduce");

        System.out.println("done");
        System.exit(0);
    }
}
