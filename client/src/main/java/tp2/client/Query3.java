package tp2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.CSVUtils;
import tpe2.api.Collators.Query3Collator;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Mappers.Query3Mapper;
import tpe2.api.Mappers.Query1Mapper;
import tpe2.api.Reducers.Query3ReducerFactory;
import tpe2.api.Reducers.SimpleReducerFactory;
import tpe2.api.Model.Flight;
import tpe2.api.Model.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static tpe2.api.CSVUtils.CSVReadFlights;

public class Query3 {

    private static final String PARTIAL_MAP = "g8-q3-pmovements";
    private static final String MOVEMENTS_MAP = "g8-q3-movements";

    private List<String> ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    public Query3() {
        super();
    }

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports"/*, required = true*/)
    private void setIps(String s) throws CmdLineException {
        List<String>  ips = Arrays.asList(s.split(","));
        for (String ip : ips) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = ips;
    }

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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Query3 query = new Query3();

        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        movPerAirPorts(query);
    }

    private static void movPerAirPorts(Query3 query) throws ExecutionException, InterruptedException {

        Logger logger = Helpers.createLoggerFor("Query3", query.getOutput()+"query3.txt");


        List<Flight> flights = null;
        try {
            logger.info("Inicio de la lectura del archivo");
            flights = CSVUtils.CSVReadFlights(query.getDir() + "movimientos.csv");
            logger.info("Fin de lectura del archivo");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        logger.info("Inicio del trabajo map/reduce");

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

        logger.info("Fin del trabajo map/reduce");

        // todo meter esto en un csv
        System.out.println("Grupo;Aeropuerto A;Aeropuerto B");
        result.forEach(e -> {
            e.getValue().forEach(t -> {
                System.out.println(e.getKey() + ";" + t.getaVal() + ";" + t.getbVal());
            });
        });
    }
}
