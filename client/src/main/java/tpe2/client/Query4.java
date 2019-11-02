package tpe2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Flight;
import tpe2.api.Collators.Query4Collator;
import tpe2.api.Mappers.Query4Mapper;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static tpe2.api.CSVUtils.CSVReadFlights;

public class Query4 {

    private String[] ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    @Option(name = "-Doaci", usage = "Origin airport")
    private String originOaci;

    @Option(name = "-Dn", usage = "Number of results")
    private String resultsAmonut;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports"/*, required = true*/)
    private void setIps(String s) throws CmdLineException {
        String[] ips = s.split(",");
        for (String ip : ips) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = ips;
    }

    public String[] getIps() {
        return ips;
    }

    public String getDir() {
        return dir;
    }

    public String getOutput() {
        return output;
    }

    public String getOriginOaci() {
        return originOaci;
    }

    public String getResultsAmonut() {
        return resultsAmonut;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Query4 query = new Query4();

        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        Logger logger = Helpers.createLoggerFor("Query4", query.getOutput()+"query4.txt");

        List<Flight> flights = new ArrayList<>();
        try {
            logger.info("Inicio de la lectura del archivo");
            flights = CSVReadFlights(query.getDir() + "/movimientos.csv");
            logger.info("Fin de lectura del archivo");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        logger.info("Inicio del trabajo map/reduce");

        final JobTracker jobTracker = hz.getJobTracker("query-4-job");

        flights = flights.stream()
                .filter(a -> a.getOaciOrigin().equals(query.originOaci))
                .collect(Collectors.toList());


        final IList<Flight> dataList = hz.getList("MOVEMENTS_MAP");
        dataList.clear();
        dataList.addAll(flights);

        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(dataList);
        final Job<String, Flight> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query4Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit(new Query4Collator(Integer.valueOf(query.resultsAmonut)));

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap("PARTIAL_MAP");
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);

        logger.info("Fin del trabajo map/reduce");

        try (PrintWriter writer = new PrintWriter(new File(query.output + "/query4.csv"))) {
            StringBuilder sb = new StringBuilder();
            Iterator it = movementsMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                sb.append(pair.getKey());
                sb.append(";");
                sb.append(pair.getValue());
                sb.append('\n');
                it.remove(); // avoids a ConcurrentModificationException
            }
            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Done");
        System.exit(0);
    }
}
