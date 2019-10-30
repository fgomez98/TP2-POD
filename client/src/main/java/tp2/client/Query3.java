package tp2.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.Option;
import tpe2.api.Flight;
import tpe2.api.Query3.MovementCountCombinerFactory;
import tpe2.api.Query3.MovementCountMapper;
import tpe2.api.Query3.MovementCountReducerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query3 {

    private static final String PARTIAL_MAP = "aca va un nombre del mapa que ni idea"; // <-- todo

    @Option(name = "-Daddresses", aliases = "--ipAddresses", usage = "one or more ip directions and ports", required = true)
    private List<String> ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    private static void movPerAirPorts() throws ExecutionException, InterruptedException {

        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        // me traigo algo data de hazelcast y se la paso al job

        JobTracker jobTracker = hz.getJobTracker("query-3-job"); // <-- todo: que onda ese nombre??

        final KeyValueSource<String, Flight> source = null; // todo: ver que valores me traigo

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        Job<String, Flight> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new MovementCountMapper())
                .combiner(new MovementCountCombinerFactory())
                .reducer(new MovementCountReducerFactory())
                .submit();

        Map<String, Long> result = future.get();

        IMap<String, Integer> resultMap = hz.getMap(PARTIAL_MAP);


    }
}
