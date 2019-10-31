package tpe2.api.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Flight;

public class MovementCountMapper implements Mapper<Integer, Flight, String, Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(Integer i, Flight flight, Context<String, Long> context) {
        // se contabilizan movimientos como despegues que tengan al aeropuerto como origen y aterrizajes que tengan al aeropuerto como destino.
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
