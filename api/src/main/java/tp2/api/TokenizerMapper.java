package tp2.api;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TokenizerMapper implements Mapper<Integer, Mov, String, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(Integer key, Mov movement, Context<String, Long> context) {
        System.out.println("Probandooooooo------------------>");
        context.emit(movement.origen, ONE);
        context.emit(movement.destino, ONE);
    }
}
