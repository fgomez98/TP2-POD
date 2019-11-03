package tpe2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import tpe2.api.CSVUtils;
import tpe2.api.Collators.Query2Collator;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;
import tpe2.api.Mappers.Query2Mapper;
import tpe2.api.Model.Tuple;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 implements Query {

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

    public Query2(int n, List<String> ips, String dir, String output) {
        this.n = n;
        this.ips = ips;
        this.dir = dir;
        this.output = output;
    }

    @Override
    public void runQuery(HazelcastInstance hazel, List<Airport> airports, List<Flight> flightList) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hazel.getJobTracker("top-" + this.getN() + "-airlines");

        IList<Flight> iList = hazel.getList("g8-q2-top-airlines");
        iList.clear();
        iList.addAll(flightList);

        Job<String, Flight> job = jobTracker.newJob(KeyValueSource.fromList(iList));
        ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Query2Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit(new Query2Collator(this.getN()));

        List<Tuple<String, Double>> result = future.get();

        try {
            CSVUtils.CSVWrite(Paths.get(this.getOutput() + "/query2.csv"),
                    result,
                    "Aerolínea;Porcentaje\n",
                    (e) -> {
                        DecimalFormat numberFormat = new DecimalFormat("#.00");
                        return e.getaVal() + ";" + numberFormat.format(e.getbVal()) + "%\n";
                    });
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
