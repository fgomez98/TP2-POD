package tp2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Combiners.SimpleChunkCombiner;
import tpe2.api.Mappers.Query1Mapper;
import tpe2.api.Reducers.Query1Reducer;
import tpe2.api.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Flight;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1 {

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

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    public String getDout() {
        return dout;
    }

    public static void main(String[] args) {
        Query1 query = new Query1();
        Logger logger = Helpers.createLoggerFor("Query1", query.getDout()+"/query1.txt");
        try {
            CmdParserUtils.init(args, query);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        for (String ip : query.getIps()) {
            System.out.println(ip);
        }

        try {
            HazelcastInstance hz = Hazelcast.newHazelcastInstance();

            logger.info("Inicio de la lectura del archivo");
            List<Airport> airports = CSVUtils.CSVReadAirports("/Users/pilo/development/itba/pod/TP2-POD/server/src/main/resources/aeropuertos.csv");
            List<Flight> flights = CSVUtils.CSVReadFlights("/Users/pilo/development/itba/pod/TP2-POD/server/src/main/resources/movimientos.csv");
            logger.info("Fin de lectura del archivo");

            logger.info("Inicio del trabajo map/reduce");
            System.out.println("OACI;Denominación;Movimientos");
            query.movPerAirPorts(hz, airports, flights).forEach(System.out::println);
            logger.info("Fin del trabajo map/reduce");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private List<String> movPerAirPorts(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        JobTracker t = hz.getJobTracker("movPerAirports");

        // primero levantamos todos los aeropuertos que nos interesa para asegurarnos que no no haya colados
        final IMap<String, String> airportsFiltered = hz.getMap("g8-q1-airportsFiltered");
        airportsFiltered.putAll(airports
                .stream()
                .filter(a -> a.getOaci() != null && !a.getOaci().equals(""))
                .collect(Collectors.toConcurrentMap(Airport::getOaci, Airport::getDenomination)));

        // ahora agregamos al multimap cada movimiento desde el aeropuerto
        final IList<Flight> flightsFiltered = hz.getList("g8-q1-flightsFiltered");
        flightsFiltered.addAll(flights.parallelStream()
                // este filtro lo tengo que hacer de ante mano si o si
                .filter(f ->
                        (f.getTypeOfMovement().equals("Despegue") && airportsFiltered.containsKey(f.getOaciOrigin()))
                                ||
                                (f.getTypeOfMovement().equals("Aterrizaje") && airportsFiltered.containsKey(f.getOaciDestination()))
                ).collect(Collectors.toList()));


        // The key returned by this KeyValueSource implementation is ALWAYS the name of the list itself,
        // whereas the value are the entries of the list, one by one.
        // https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/KeyValueSource.html
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsFiltered);
        Job<String, Flight> job = t.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                // no es necesario eliminar las llaves que no esté en airports.csv, pues ya fue hecho
                // por cada origen y destino del Flight emitimos un 1
                .mapper(new Query1Mapper())
                // antes de emitir la llave por la red "reducimos" localmente para minimizar los datos que se envian por la red
                .combiner(new SimpleChunkCombiner())
                // aeropuertos sin vuelos no llegan a persistirse
                .reducer(new Query1Reducer())
                .submit();

        // Wait and retrieve the result
        Map<String, Long> result = future.get();
        return result.entrySet()
                .stream()
                .sorted((o1, o2) -> o1.getValue().equals(o2.getValue()) ?
                        o1.getKey().compareTo(o2.getKey()):
                        o2.getValue().compareTo(o1.getValue()))
                .map(e -> e.getKey() +";"+ airportsFiltered.get(e.getKey()) +";"+ e.getValue())
                .collect(Collectors.toList());
    }
}
