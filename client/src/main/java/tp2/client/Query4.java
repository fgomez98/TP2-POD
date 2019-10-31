package tp2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Model.Flight;
import tpe2.api.Collators.Query4Collator;
import tpe2.api.Combiners.Query4CombinerFactory;
import tpe2.api.Mappers.Query4Mapper;
import tpe2.api.Reducers.Query4ReducerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static tpe2.api.CSVUtils.CSVReadFlights;

public class Query4 {

    @Option(name = "-Daddresses", aliases = "--ipAddresses", usage = "one or more ip directions and ports", required = false)
    private String[] ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    @Option(name = "-Doaci", usage = "Origin airport")
    private String originOaci;

    @Option(name = "-Dn", usage = "Number of results")
    private String resultsAmonut;

    private void setIps(String s) throws CmdLineException {
        String[] ips = s.split(",");
        for (String ip : ips) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = ips;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Query4 query = new Query4();

        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        List<Flight> flights = null;
        try {
            flights = CSVReadFlights(query.dir + "/movimientos.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

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
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(Integer.valueOf(query.resultsAmonut)));

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap("PARTIAL_MAP");
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);


        System.out.println(movementsMap);
    }
}
