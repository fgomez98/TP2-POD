package tpe2.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Reducers.SimpleReducerFactory;
import tpe2.api.Mappers.Query6Mapper;
import tpe2.api.Model.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Flight;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query6 implements Query {

    private List<String> ips;

    private String din;

    private String dout;

    private Long dmin;

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    public String getDout() {
        return dout;
    }

    public Long getDmin() {
        return dmin;
    }

    public Query6(List<String> ips, String din, String dout, Long dmin) {
        this.ips = ips;
        this.din = din;
        this.dout = dout;
        this.dmin = dmin;
    }

    private List<String> sharedMovements(HazelcastInstance hz, List<Airport> airports, List<Flight> flights, Long min) throws ExecutionException, InterruptedException {
        JobTracker t = hz.getJobTracker("sharedMovements");

        // primero levantamos todos los aeropuertos que nos interesa para asegurarnos que no no haya colados
        final Map<String, String> airportsFiltered = new HashMap<>(airports
                .parallelStream()
                .collect(Collectors.toConcurrentMap(Airport::getOaci, Airport::getProvince)));

        // ahora agregamos al multimap cada movimiento desde el aeropuerto
        final IList<Flight> flightsFiltered = hz.getList("g8-q6-flightsFiltered");
        flightsFiltered.clear();
        flightsFiltered.addAll(flights.parallelStream()
                .filter(f -> !f.getFlightCLassification().equals("Internacional")
                        && airportsFiltered.containsKey(f.getOaciOrigin())
                        && airportsFiltered.containsKey(f.getOaciDestination())
                )
                // peek() can be useful in another scenario:
                // when we want to alter the inner state of an element.
                .peek(f -> {
                    f.setOaciOrigin(airportsFiltered.get(f.getOaciOrigin()));
                    f.setOaciDestination(airportsFiltered.get(f.getOaciDestination()));
                }).collect(Collectors.toList()));


        // The key returned by this KeyValueSource implementation is ALWAYS the name of the list itself,
        // whereas the value are the entries of the list, one by one.
        // https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/KeyValueSource.html
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsFiltered);
        Job<String, Flight> job = t.newJob(source);
        ICompletableFuture<List<String>> future = job
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
                                .sorted((o1, o2) -> o1.getValue().equals(o2.getValue()) ?
                                        o1.getKey().compareTo(o2.getKey()) :
                                        o2.getValue().compareTo(o1.getValue()))
                                .map(e -> e.getKey() + ";" + e.getValue() + "\n")
                                .collect(Collectors.toList())
                );

        // Wait and retrieve the result
        List<String> result = future.get();
        return result;
    }

    @Override
    public void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        try {
            CSVUtils.CSVWrite(Paths.get(this.getDout() + "/query6.csv"),
                    sharedMovements(hz, airports, flights, this.getDmin()),
                    "Provincia A;Provincia B;Movimientos\n",
                    e -> e
            );
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
