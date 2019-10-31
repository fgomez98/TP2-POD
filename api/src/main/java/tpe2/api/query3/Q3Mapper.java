package tpe2.api.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Q3Mapper implements Mapper<String, Long, Long, String> {
    @Override
    public void map(String key, Long value, Context<Long, String> context) {
        value /= 1000;
        value *= 1000;
        if (value > 0) {
            context.emit(value, key);
        }
    }
}
