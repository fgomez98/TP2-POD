package tpe2.api.query5;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Flight;

public class Q5Mapper implements Mapper<String, Flight, String, Long> {

    private static final Long PRIVATE = 1L;
    private static final Long NON_PRIVATE = 0L;

    /*
        Emite Origen OACI o Destino OACI segun si el vuleo es privado o no
     */
    @Override
    public void map(String key, Flight flight, Context<String, Long> context) {
        switch (flight.getFlightClass()) {
            case "Despegue":
                if (isPrivate(flight.getFlightClass())) {
                    context.emit(flight.getOaciOrigin(), PRIVATE);
                } else {
                    context.emit(flight.getOaciOrigin(), NON_PRIVATE);
                }
                break;
            case "Aterrizaje":
                if (isPrivate(flight.getFlightClass())) {
                    context.emit(flight.getOaciDestination(), PRIVATE);
                } else {
                    context.emit(flight.getOaciDestination(), NON_PRIVATE);
                }
                break;
        }
    }

    private boolean isPrivate(String flightClass) {
        if (flightClass.compareTo("Vuelo Privado con Matrícula Nacional") == 0 ||
                flightClass.compareTo("Vuelo Privado con Matrícula Extranjera") == 0) {
            return true;
        }
        return false;
    }
}
