package tpe2.api.query4;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Flight;

public class Query4Mapper implements Mapper<String, Flight, String, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        if (flight.getTypeOfMovement().equals("Despegue")) {
            context.emit(flight.getOaciDestination(), ONE);
        }
    }
}
