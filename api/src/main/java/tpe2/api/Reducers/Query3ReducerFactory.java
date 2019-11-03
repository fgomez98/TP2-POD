package tpe2.api.Reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import tpe2.api.Model.Tuple;

import java.util.ArrayList;
import java.util.List;

public class Query3ReducerFactory implements ReducerFactory<Long, String, List<Tuple<String, String>>> {

    @Override
    public Reducer<String, List<Tuple<String, String>>> newReducer(Long aLong) {
        return new Q3Reducer();
    }

    /*
        Agrupa en tuplas de oaci y las ordena alfabeticamente sin repetir pares
     */
    private class Q3Reducer extends Reducer<String, List<Tuple<String, String>>> {

        private List<String> oacis;

        @Override
        public void beginReduce() {
            super.beginReduce();
            oacis = new ArrayList<>();
        }

        @Override
        public void reduce(String s) {
            oacis.add(s);
        }

        @Override
        public List<Tuple<String, String>> finalizeReduce() {
            // lista con pares de oaci sin repetir combinaciones
            oacis.sort(String::compareTo);
            List<Tuple<String, String>> list = new ArrayList<>();
            for (int i = 0; i < oacis.size(); i++) {
                for (int j = i + 1; j < oacis.size(); j++) {
                    list.add(new Tuple<>(oacis.get(i), oacis.get(j)));
                }
            }
            return list;
        }
    }
}
