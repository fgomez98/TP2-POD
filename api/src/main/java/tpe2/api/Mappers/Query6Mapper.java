package tpe2.api.Mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Flight;

public class Query6Mapper implements Mapper<String, Flight, String, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        int comp = flight.getOaciOrigin().compareTo(flight.getOaciDestination());
        if (comp < 0) {
            context.emit(flight.getOaciOrigin()+";"+flight.getOaciDestination(), ONE);
        } else if (comp > 0) {
            context.emit(flight.getOaciDestination()+";"+flight.getOaciOrigin(), ONE);
        } // else { // comp == 0 considerado
    }
}
