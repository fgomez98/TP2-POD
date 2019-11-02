package tpe2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Reducers.SimpleReducerFactory;
import tpe2.api.Mappers.Query6Mapper;
import tpe2.api.Model.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Flight;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query6 {

    private List<String> ips;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports", required = true)
    private void setIps(String s) throws CmdLineException {
        List<String> list = Arrays.asList(s.split(";"));
        for (String ip : list) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = list;
    }


    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String din;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String dout;

    @Option(name = "-Dmin", aliases = "--minCount", usage = "Minimum number of shared movements to show")
    private String dmin;

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    public String getDout() {
        return dout;
    }

    public String getDmin() {
        return dmin;
    }

    public static void main(String[] args) {
        Query6 query = new Query6();
        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        Logger logger = Helpers.createLoggerFor("Query6", query.getDout()+"/query6.txt");

        for (String ip : query.getIps()) {
            System.out.println(ip);
        }

        try {
            HazelcastInstance hz = Hazelcast.newHazelcastInstance();

            logger.info("Inicio de la lectura del archivo");
            List<Airport> airports = CSVUtils.CSVReadAirports(query.getDout() + "aeropuertos.csv");
            List<Flight> flights = CSVUtils.CSVReadFlights(query.getDout() + "movimientos.csv");
            logger.info("Fin de lectura del archivo");

            logger.info("Inicio del trabajo map/reduce");
            System.out.println("Provincia A;Provincia B;Movimientos");
            query.sharedMovements(hz, airports, flights, Long.parseLong(query.getDmin())).forEach(System.out::println);
            logger.info("Fin del trabajo map/reduce");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> sharedMovements(HazelcastInstance hz, List<Airport> airports, List<Flight> flights, Long min) throws ExecutionException, InterruptedException {
        JobTracker t = hz.getJobTracker("sharedMovements");

        // primero levantamos todos los aeropuertos que nos interesa para asegurarnos que no no haya colados
        final Map<String, String> airportsFiltered = new HashMap<>(airports
                .parallelStream()
                .filter(a -> a.getOaci() != null && !a.getOaci().equals(""))
                .collect(Collectors.toConcurrentMap(Airport::getOaci, Airport::getProvince)));

        // ahora agregamos al multimap cada movimiento desde el aeropuerto
        final IList<Flight> flightsFiltered = hz.getList("g8-q6-flightsFiltered");
        flightsFiltered.addAll(flights.parallelStream()
                .filter(f -> !f.getFlightCLassification().equals("Internacional")
                            && airportsFiltered.containsKey(f.getOaciOrigin())
                                && airportsFiltered.containsKey(f.getOaciDestination())
                        )
                //peek() can be useful in another scenario:
                // when we want to alter the inner state of an element.
                .peek(f -> {
                        f.setOaciOrigin(airportsFiltered.get(f.getOaciOrigin()));
                        f.setOaciDestination(airportsFiltered.get(f.getOaciDestination()));
                }).collect(Collectors.toList()));


        // The key returned by this KeyValueSource implementation is ALWAYS the name of the list itself,
        // whereas the value are the entries of the list, one by one.
        // https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/KeyValueSource.html
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsFiltered);
        Job<String, Flight> job = t.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                // por cada origen y destino del Flight emitimos un 1 apra la llame origen;destino o destino;origen según orden
                .mapper(new Query6Mapper())
                // antes de emitir la llave por la red "reducimos" localmente para minimizar los datos que se envian por la red
                .combiner(new SimpleChunkCombinerFactory())
                // aeropuertos sin vuelos no llegan a persistirse
                .reducer(new SimpleReducerFactory())
                // filtramos todos los que la sume de menor que min
                .submit(iterable ->
                    StreamSupport.stream(iterable.spliterator(), false)
                            .filter(e -> e.getValue().compareTo(min) >= 0)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );

        // Wait and retrieve the result
        Map<String, Long> result = future.get();
        return result.entrySet()
                .stream()
                .sorted((o1, o2) -> o1.getValue().equals(o2.getValue()) ?
                        o1.getKey().compareTo(o2.getKey()):
                        o2.getValue().compareTo(o1.getValue()))
                .map(e -> e.getKey() +";"+ e.getValue())
                .collect(Collectors.toList());
    }
}
