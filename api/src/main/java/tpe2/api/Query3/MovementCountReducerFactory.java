package tpe2.api.Query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class MovementCountReducerFactory implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String s) {
        return new Q3Reducer();
    }


    private class Q3Reducer extends Reducer<Long, Long> {

        private long count;

        @Override
        public void beginReduce () {
            count = 0;
        }

        @Override
        public void reduce(Long aLong) {
            count++;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
