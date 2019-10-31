package tp2.client;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Flight;
import tpe2.api.query3.*;
import tpe2.api.Tuple;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static tpe2.api.CSVUtils.CSVRead;

public class Query3 {

    private static final String PARTIAL_MAP = "map2"; // <-- todo
    private static final String MOVEMENTS_MAP = "map1"; // <-- todo

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
            flights = CSVRead(Paths.get("/Users/fermingomez/Desktop/movimientos.csv"), Flight.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Config cfg = new Config();
        NetworkConfig network = cfg.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().addMember("127.0.0.1");
        join.getTcpIpConfig().setEnabled(true);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

//        final ClientConfig ccfg = new ClientConfig();
//        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        final JobTracker jobTracker = hz.getJobTracker("query-3-job"); // <-- todo: que onda ese nombre??

        final IMap<Integer, Flight> dataList = hz.getMap(MOVEMENTS_MAP);
        dataList.clear();
        for (int i = 0; i < flights.size(); i++) {
            dataList.set(i, flights.get(i));
        }

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<Integer, Flight> source = KeyValueSource.fromMap(dataList);
        final Job<Integer, Flight> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new MovementCountMapper())
                .combiner(new MovementCountCombinerFactory())
                .reducer(new MovementCountReducerFactory())
                .submit();

        Map<String, Long> movementsMap = future.get();

        IMap<String, Long> movementsIMap = hz.getMap(PARTIAL_MAP);
        movementsIMap.clear();
        movementsMap.forEach(movementsIMap::set);

        final KeyValueSource<String, Long> movementSource = KeyValueSource.fromMap(movementsIMap);
        final Job<String, Long> job2 = jobTracker.newJob(movementSource);
        final ICompletableFuture<List<Map.Entry<Long, List<Tuple<String, String>>>>> future2 = job2
                .mapper(new Q3Mapper())
                // vale la pena meter un combiner que vaya metiedo agregando los valores en una lista??
                .reducer(new Q3ReducerFactory())
                .submit(new Q3Collator());

        List<Map.Entry<Long, List<Tuple<String, String>>>> result = future2.get();

        // todo meter esto en un csv
        result.forEach(e -> {
            e.getValue().forEach(t -> {
                System.out.println(e.getKey() + ";" + t.getaVal() + ";" + t.getbVal());
            });
        });
    }
}
