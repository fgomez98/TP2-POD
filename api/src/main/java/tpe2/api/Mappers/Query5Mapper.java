package tpe2.api.Mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Model.Flight;

import java.util.List;

public class Query5Mapper implements Mapper<String, Flight, String, Long> {

    private static final Long PRIVATE = 1L;
    private static final Long NON_PRIVATE = 0L;

    private List<String> oacis;

    public Query5Mapper(List<String> oacis) {
        this.oacis = oacis;
    }

    /*
        Emite Origen OACI o Destino OACI segun si el vuleo es privado o no y se encuentra en la lista de aeropuertos
    */
    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        if (oacis.contains(flight.getOaciOrigin())) {
            context.emit(flight.getOaciOrigin(), isPrivate(flight.getFlightClass()));
        }
        if (oacis.contains(flight.getOaciDestination())) {
            context.emit(flight.getOaciDestination(), isPrivate(flight.getFlightClass()));
        }
    }

    private Long isPrivate(String flightClass) {
        return (flightClass.compareTo("Vuelo Privado con Matrícula Nacional") == 0 ||
                flightClass.compareTo("Vuelo Privado con Matrícula Extranjera") == 0)
                ? PRIVATE : NON_PRIVATE;
    }
}
