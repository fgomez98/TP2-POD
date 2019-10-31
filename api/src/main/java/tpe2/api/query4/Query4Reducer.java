package tpe2.api.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import query1.Query1Reducer;

public class Query4Reducer implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key ) {
        return new Query4Reducer.FlightsCounter();
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