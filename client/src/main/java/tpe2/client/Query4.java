package tpe2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import tpe2.api.CSVUtils;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;
import tpe2.api.Collators.Query4Collator;
import tpe2.api.Mappers.Query4Mapper;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4 implements Query {

    private String dir;

    private String output;

    private String originOaci;

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

        try {
            CSVUtils.CSVWrite(Paths.get(this.getOutput() + "/query4.csv"),
                    movementsMap.entrySet(),
                    "OACI;Despegues\n",
                    e -> e.getKey() + ";" + e.getValue() + "\n"
            );
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
