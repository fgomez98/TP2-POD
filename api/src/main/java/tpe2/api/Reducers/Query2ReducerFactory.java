package tpe2.api.Reducers;


import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


public class Query2ReducerFactory implements ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String s) {
        return new Query2Reducer();
    }

    private class Query2Reducer extends Reducer<Long, Long>{

        private volatile long sum;

        @Override
        public void beginReduce () {
            sum = 0;
        }

        @Override
        public void reduce(Long aLong) {
            sum += aLong;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
