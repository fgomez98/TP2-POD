package tp2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.aggregation.impl.LongSumAggregation;
import query1.Query1Combiner;
import query1.Query1Mapper;
import query1.Query1Reducer;
import tpe2.api.Airport;
import tpe2.api.Flight;
import tpe2.api.Tuple;
import tpe2.api.query3.Q3Collator;
import tpe2.api.query3.Q3Mapper;
import tpe2.api.query3.Q3ReducerFactory;
import tpe2.api.query5.Q5Collator;
import tpe2.api.query5.Q5CombinerFactory;
import tpe2.api.query5.Q5Mapper;
import tpe2.api.query5.Q5ReducerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static tpe2.api.CSVUtils.CSVReadAirports;
import static tpe2.api.CSVUtils.CSVReadFlights;

public class Query5 {

    private static final String AIRPORTS_MAP = "g8-q5-airports";
    private static final String MOVEMENTS_MAP = "g8-q5-movements";

    private static void q5() throws ExecutionException, InterruptedException {

        List<Flight> flights = null;
        List<Airport> airports = null;
        try {
            flights = CSVReadFlights("/Users/fermingomez/Desktop/movimientos.csv");
            airports = CSVReadAirports("/Users/fermingomez/Desktop/aeropuertos.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        final JobTracker jobTracker = hz.getJobTracker("query-5-job");

        final IList<Flight> movementsList = hz.getList(MOVEMENTS_MAP);
        movementsList.clear();
        movementsList.addAll(flights);
        final IList<Airport> airportsList = hz.getList(AIRPORTS_MAP);
        airportsList.clear();
        airportsList.addAll(airports);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<String, Airport> airportSource = KeyValueSource.fromList(airportsList);
        final KeyValueSource<String, Flight> movementSource = KeyValueSource.fromList(movementsList);

        LongSumAggregation laddr = new LongSumAggregation();


        int n = 5; // todo param

        final Job<String, Flight> job = jobTracker.newJob(movementSource);
        final ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new Q5Mapper())
                .combiner(new Q5CombinerFactory())
                .reducer(new Q5ReducerFactory())
                .submit(new Q5Collator(n));

        List<Map.Entry<String, Double>> movementsMap = future.get();

        // solo me interesan los aeropuertos presentes
        movementsMap.removeIf((e) -> !airportSource.getAllKeys().contains(e.getKey()));
        System.out.println("OACI;Porcentaje");
        movementsMap.forEach(e -> System.out.println(e.getKey() + ";" + e.getValue()));


    }
}
