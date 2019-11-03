package tpe2.api.Mappers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;

public class Query1Mapper implements Mapper<String, Flight, String, Long>, HazelcastInstanceAware {
    private static final Long ONE = 1L;
    private transient HazelcastInstance hz;

    private static final String AIRPORTSMAP = "g8-q1-airportsFiltered";

    @Override
    public void map(String key, Flight f, Context<String, Long> context) {
        IMap<String, String> airports = hz.getMap(AIRPORTSMAP);
        switch (f.getTypeOfMovement()) {
            case "Despegue":
                if (airports.containsKey(f.getOaciOrigin()))
                    context.emit(f.getOaciOrigin(), ONE);
                break;
            case "Aterrizaje":
                if (airports.containsKey(f.getOaciDestination()))
                    context.emit(f.getOaciDestination(), ONE);
                break;
        }

    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}
