package tpe2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import tpe2.api.Collections;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.KeyPredicates.OnlyOACIAirports;
import tpe2.api.Mappers.Query1Mapper;
import tpe2.api.Reducers.SimpleReducerFactory;
import tpe2.api.Model.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Flight;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1 implements Query {

    private List<String> ips;

    private String din;

    private String dout;

    public List<String> getIps() {
        return ips;
    }

    public String getDin() {
        return din;
    }

    private String getDout() {
        return dout;
    }

    Query1(List<String> ips, String din, String dout) {
        this.ips = ips;
        this.din = din;
        this.dout = dout;
    }

    private List<String> movPerAirPorts(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        JobTracker t = hz.getJobTracker("movPerAirports");
        // primero mapeamos los aeropuertos de manera que nos sirva en el futuro
        final IMap<String, String> airports2 = hz.getMap(Collections.airports.getName());
        airports2.putAll(airports
                .stream()
                .collect(Collectors.toMap(Airport::getOaci, Airport::getDenomination)));

        // ahora agregamos al multimap cada movimiento desde el aeropuerto
        final MultiMap<String, Flight> flights2 = hz.getMultiMap(Collections.flights.getName());
        flights2.clear(); flights.forEach(f -> {
            //if (f.getTypeOfMovement().equals("Despegue"))
                flights2.put(f.getOaciOrigin(), f);
            //else
                flights2.put(f.getOaciDestination(), f);
        });

        // The key returned by this KeyValueSource implementation is ALWAYS the name of the list itself,
        // whereas the value are the entries of the list, one by one.
        // https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/KeyValueSource.html
        final KeyValueSource<String, Flight> source = KeyValueSource.fromMultiMap(flights2);
        Job<String, Flight> job = t.newJob(source);
        ICompletableFuture<List<String>> future = job
                .keyPredicate(new OnlyOACIAirports())
                // por cada origen y destino del Flight emitimos un 1
                .mapper(new Query1Mapper())
                // antes de emitir la llave por la red "reducimos" localmente para minimizar los datos que se envian por la red
                .combiner(new SimpleChunkCombinerFactory())
                // aeropuertos sin vuelos no llegan a persistirse
                .reducer(new SimpleReducerFactory())
                .submit(iterable ->
                        StreamSupport.stream(iterable.spliterator(), false)
                        .sorted((o1, o2) -> o1.getValue().equals(o2.getValue()) ?
                                o1.getKey().compareTo(o2.getKey()) :
                                o2.getValue().compareTo(o1.getValue()))
                        .map(e -> e.getKey() + ";" + airports2.get(e.getKey())+ ";" + e.getValue() + "\n")
                        .collect(Collectors.toList()));
        // Wait and retrieve the result
        return future.get();
    }

    @Override
    public void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
        try {
            CSVUtils.CSVWrite(Paths.get(this.getDout() + "/query1.csv"),
                    movPerAirPorts(hz, airports, flights),
                    "OACI;Denominación;Movimientos\n",
                    e -> e
            );
        } catch (IOException e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
