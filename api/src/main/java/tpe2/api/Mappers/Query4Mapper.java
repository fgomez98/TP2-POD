package tpe2.api.Mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Model.Flight;

public class Query4Mapper implements Mapper<String, Flight, String, Long> {
    private static final Long ONE = 1L;

    private String oaciOrigin;

    public Query4Mapper(String oaciOrigin) {
        this.oaciOrigin = oaciOrigin;
    }

    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        if (flight.getTypeOfMovement().equals("Despegue") && flight.getOaciOrigin().equals(oaciOrigin)) {
            context.emit(flight.getOaciDestination(), ONE);
        }
    }
}
