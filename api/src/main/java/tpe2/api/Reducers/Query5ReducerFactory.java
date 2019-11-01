package tpe2.api.Reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import tpe2.api.Model.Tuple;


public class Query5ReducerFactory implements ReducerFactory<String, Tuple<Long, Long>, Double> {

    @Override
    public Reducer<Tuple<Long, Long>, Double> newReducer(String s) {
        return new Q3Reducer();
    }

    private class Q3Reducer extends Reducer<Tuple<Long, Long>, Double> {

        private long totalCount;
        private long privateCount;

        @Override
        public void beginReduce() {
            super.beginReduce();
            totalCount = 0;
            privateCount = 0;
        }

        @Override
        public void reduce(Tuple<Long, Long> tuple) {
            privateCount += tuple.getaVal();
            totalCount += tuple.getbVal();
        }

        @Override
        public Double finalizeReduce() {
            double percentage = 100 * (privateCount / (double) totalCount);
            return Math.floor(percentage * 100) / 100; // truncado a 2 decimales
        }
    }
}
