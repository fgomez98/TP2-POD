package query1;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tpe2.api.Airport;
import tpe2.api.Flight;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1 {
    public List<String> movPerAirPorts(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException {
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
                .combiner(new Query1Combiner())
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
