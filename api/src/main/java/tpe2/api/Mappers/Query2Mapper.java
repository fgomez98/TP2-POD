package tpe2.api.Mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Flight;

public class Query2Mapper implements Mapper<String, Flight, String, Long> {

    private static final Long ONE = 1L;

    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        if (flight.getFlightCLassification().equals("Cabotaje")) {
            context.emit(flight.getAirline(), ONE);
        }
    }
}
