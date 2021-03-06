package tpe2.api.Mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Model.Flight;

public class Query3MovementMapper implements Mapper<String, Flight, String, Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        switch (flight.getTypeOfMovement()) {
            case "Despegue":
                context.emit(flight.getOaciOrigin(), ONE);
                break;
            case "Aterrizaje":
                context.emit(flight.getOaciDestination(), ONE);
                break;
        }
    }
}
