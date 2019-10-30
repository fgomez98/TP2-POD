package tpe2.api.Query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class MovementCountCombinerFactory implements CombinerFactory<String, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(String s) {
        return new MovementCountCombiner();
    }

    private class MovementCountCombiner extends Combiner<Long, Long> {

        private long count;

        @Override
        public void reset() {
            super.reset();
        }

        @Override
        public void beginCombine() {
            super.beginCombine();
            count = 0;
        }

        @Override
        public void combine(Long aLong) {
            count++;
        }

        @Override
        public Long finalizeChunk() {
            return count;
        }
    }
}
