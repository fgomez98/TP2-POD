package tpe2.client;


import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import tpe2.api.CSVUtils;
import tpe2.api.Collators.Query3Collator;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Mappers.Query3Mapper;
import tpe2.api.Mappers.Query1Mapper;
import tpe2.api.Model.Airport;
import tpe2.api.Reducers.Query3ReducerFactory;
import tpe2.api.Reducers.SimpleReducerFactory;
import tpe2.api.Model.Flight;
import tpe2.api.Model.Tuple;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query3 implements Query {

    private static final String PARTIAL_MAP = "g8-q3-pmovements";
    private static final String MOVEMENTS_MAP = "g8-q3-movements";

    private List<String> ips;

    private String dir;

    private String output;

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public Query3(List<String> ips, String dir, String output) {
        this.ips = ips;
        this.dir = dir;
        this.output = output;
    }

    @Override
    public void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = hz.getJobTracker("query-3-job");

        final IList<Flight> dataList = hz.getList(MOVEMENTS_MAP);
        dataList.clear();
        dataList.addAll(flights);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(dataList);
        final Job<String, Flight> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit();

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap(PARTIAL_MAP);
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);

        final KeyValueSource<String, Long> source2 = KeyValueSource.fromMap(movementsIMap);
        final Job<String, Long> job2 = jobTracker.newJob(source2);
        final ICompletableFuture<List<Map.Entry<Long, List<Tuple<String, String>>>>> future2 = job2
                .mapper(new Query3Mapper())
                // vale la pena meter un combiner que vaya metiedo agregando los valores en una lista??
                .reducer(new Query3ReducerFactory())
                .submit(new Query3Collator());

        List<Map.Entry<Long, List<Tuple<String, String>>>> result = future2.get();

        try {
            CSVUtils.CSVWrite(Paths.get(this.getOutput() + "/query3.csv"),
                    result,
                    "Grupo;Aeropuerto A;Aeropuerto B\n",
                    this::toCSVRow
            );
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }

    private String toCSVRow(Map.Entry<Long, List<Tuple<String, String>>> entry) {
        StringBuilder sb = new StringBuilder();
        entry.getValue().forEach(tuple ->
                sb.append(entry.getKey()).append(';').append(tuple.getaVal()).append(';').append(tuple.getbVal()).append('\n')
        );
        return sb.toString();
    }
}
