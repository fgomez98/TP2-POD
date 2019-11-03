package tpe2.client;

import com.hazelcast.core.HazelcastInstance;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface Query {

    void runQuery(HazelcastInstance hz, List<Airport> airports, List<Flight> flights) throws ExecutionException, InterruptedException;
}
