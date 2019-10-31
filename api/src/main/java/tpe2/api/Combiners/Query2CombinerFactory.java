package tpe2.api.Combiners;


import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2CombinerFactory implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String s) {
        return new Query2Combiner();
    }

    private class Query2Combiner extends Combiner<Long, Long> {
        private long chunkSum;

        @Override
        public void combine(Long value) {
            chunkSum += value;
        }

        @Override
        public Long finalizeChunk() {
            return chunkSum;
        }

        @Override
        public void reset() {
            chunkSum = 0;
        }
    }
}
