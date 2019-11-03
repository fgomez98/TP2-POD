package tpe2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import tpe2.api.Model.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Flight;
import tpe2.api.Model.Tuple;
import tpe2.api.Collators.Query5Collator;
import tpe2.api.Combiners.Query5CombinerFactory;
import tpe2.api.Mappers.Query5Mapper;
import tpe2.api.Reducers.Query5ReducerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Query5 implements Query {

    private static final String MOVEMENTS_MAP = "g8-q5-movements";

    private int n;

    private List<String> ips;

    private String dir;

    private String output;

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public List<String> getIps() {
        return ips;
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

    public Query5(int n, List<String> ips, String dir, String output) {
        this.n = n;
        this.ips = ips;
        this.dir = dir;
        this.output = output;
    }

    @Override
    public void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flightList) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("query-5-job");

        final IList<Flight> movementsList = hz.getList(MOVEMENTS_MAP);
        movementsList.clear();
        movementsList.addAll(flightList);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<String, Flight> movementSource = KeyValueSource.fromList(movementsList);

        List<String> oacis = new ArrayList<>();
        airports.forEach((a) -> {
            if (a.getOaci() != null && !a.getOaci().equals("")) {
                oacis.add(a.getOaci());
            }
        });

        final Job<String, Flight> job = jobTracker.newJob(movementSource);
        final ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Query5Mapper(oacis))
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator(this.getN()));

        List<Tuple<String, Double>> result = future.get();

        try {
            DecimalFormat numberFormat = new DecimalFormat("##0.00");
            CSVUtils.CSVWrite(Paths.get(this.getOutput() + "/query5.csv"),
                    result,
                    "OACI;Porcentaje\n",
                    tuple -> tuple.getaVal() + ";" + numberFormat.format(tuple.getbVal()) + "%\n"
            );
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
