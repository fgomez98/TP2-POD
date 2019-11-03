package tpe2.api.Combiners;


import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import tpe2.api.Model.Tuple;

public class Query5CombinerFactory implements CombinerFactory<String, Long, Tuple<Long, Long>> {

    @Override
    public Combiner<Long, Tuple<Long, Long>> newCombiner(String s) {
        return new Q5CountCombiner();
    }

    class Q5CountCombiner extends Combiner<Long, Tuple<Long, Long>> {

        private long totalCount;
        private long privateCount;

        @Override
        public void beginCombine() {
            super.beginCombine();
            totalCount = 0;
            privateCount = 0;
        }

        @Override
        public void combine(Long value) {
            totalCount++;
            privateCount += value;
        }

        @Override
        public Tuple<Long, Long> finalizeChunk() {
            return new Tuple<>(privateCount, totalCount);
        }

        @Override
        public void reset() {
            totalCount = 0;
            privateCount = 0;
        }
    }
}
