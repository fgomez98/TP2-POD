package tpe2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.Option;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;
import tpe2.api.Collators.Query4Collator;
import tpe2.api.Mappers.Query4Mapper;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4 implements Query {

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    @Option(name = "-Doaci", usage = "Origin airport")
    private String originOaci;

    @Option(name = "-Dn", usage = "Number of results")
    private int resultsAmonut;

    private List<String> ips;

    public List<String> getIps() {
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

    public int getResultsAmonut() {
        return resultsAmonut;
    }

    public Query4(List<String> ips, String dir, String output, String originOaci, int resultsAmonut) {
        this.ips = ips;
        this.dir = dir;
        this.output = output;
        this.originOaci = originOaci;
        this.resultsAmonut = resultsAmonut;
    }

    @Override
    public void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = hz.getJobTracker("query-4-job");

        flights = flights.stream()
                .filter(a -> a.getOaciOrigin().equals(this.originOaci))
                .collect(Collectors.toList());

        final IList<Flight> dataList = hz.getList("g8-q4-movements-map");
        dataList.clear();
        dataList.addAll(flights);

        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(dataList);
        final Job<String, Flight> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query4Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit(new Query4Collator(this.resultsAmonut));

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap("g8-q4-partial-map");
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);

        try (PrintWriter writer = new PrintWriter(new File(this.getOutput() + "/query4.csv"))) {
            writer.write("OACI;Despegues\n");
            StringBuilder sb = new StringBuilder();
            Iterator it = movementsMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
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
    }
}
