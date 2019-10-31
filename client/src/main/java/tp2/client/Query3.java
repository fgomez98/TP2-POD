package tp2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import query1.Query1Combiner;
import query1.Query1Mapper;
import query1.Query1Reducer;
import tpe2.api.Flight;
import tpe2.api.query3.*;
import tpe2.api.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static tpe2.api.CSVUtils.CSVReadFlights;

public class Query3 {

    private static final String PARTIAL_MAP = "g8-q3-pmovements";
    private static final String MOVEMENTS_MAP = "g8-q3-movements";

    @Option(name = "-Daddresses", aliases = "--ipAddresses", usage = "one or more ip directions and ports", required = true)
    private String[] ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

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
        movPerAirPorts();
    }

    private static void movPerAirPorts() throws ExecutionException, InterruptedException {

        List<Flight> flights = null;
        try {
            flights = CSVReadFlights("/Users/fermingomez/Desktop/movimientos.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        final JobTracker jobTracker = hz.getJobTracker("query-3-job");

        final IList<Flight> dataList = hz.getList(MOVEMENTS_MAP);
        dataList.clear();
        dataList.addAll(flights);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(dataList);
        final Job<String, Flight> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1Combiner())
                .reducer(new Query1Reducer())
                .submit();

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap(PARTIAL_MAP);
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);

        final KeyValueSource<String, Long> source2 = KeyValueSource.fromMap(movementsIMap);
        final Job<String, Long> job2 = jobTracker.newJob(source2);
        final ICompletableFuture<List<Map.Entry<Long, List<Tuple<String, String>>>>> future2 = job2
                .mapper(new Q3Mapper())
                // vale la pena meter un combiner que vaya metiedo agregando los valores en una lista??
                .reducer(new Q3ReducerFactory())
                .submit(new Q3Collator());

        List<Map.Entry<Long, List<Tuple<String, String>>>> result = future2.get();

        // todo meter esto en un csv
        System.out.println("Grupo;Aeropuerto A;Aeropuerto B");
        result.forEach(e -> {
            e.getValue().forEach(t -> {
                System.out.println(e.getKey() + ";" + t.getaVal() + ";" + t.getbVal());
            });
        });
    }
}
