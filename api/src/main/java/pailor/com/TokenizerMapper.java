package pailor.com;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TokenizerMapper implements Mapper<String, Mov, String, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(String key, Mov movement, Context<String, Long> context) {
        context.emit(movement.origen, ONE);
        context.emit(movement.destino, ONE);
    }
}
