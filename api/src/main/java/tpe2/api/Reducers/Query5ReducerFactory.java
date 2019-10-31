package tpe2.api.query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import tpe2.api.Model.Tuple;


public class Q5ReducerFactory implements ReducerFactory<String, Tuple<Long, Long>, Tuple<Long, Long>> {

    @Override
    public Reducer<Tuple<Long, Long>, Tuple<Long, Long>> newReducer(String s) {
        return new Q3Reducer();
    }

    private class Q3Reducer extends Reducer<Tuple<Long, Long>, Tuple<Long, Long>> {

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
        public Tuple<Long, Long> finalizeReduce() {
            return new Tuple<>(privateCount, totalCount);
        }
    }
}
