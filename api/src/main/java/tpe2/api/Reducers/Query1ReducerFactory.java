package tpe2.api.Reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1Reducer implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key ) {
        return new FlightsCounter();
    }
    private class FlightsCounter extends Reducer<Long, Long> {
        private volatile long sum;
        @Override
        public void beginReduce () {
            sum = 0;
        }
        @Override
        public void reduce( Long value ) {
            sum += value.longValue();
        }
        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}