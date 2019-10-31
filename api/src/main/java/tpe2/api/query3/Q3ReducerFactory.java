package tpe2.api.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import tpe2.api.Tuple;

import java.util.ArrayList;
import java.util.List;

public class Q3ReducerFactory implements ReducerFactory<Long, String, List<Tuple<String, String>>> {

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
            List<Tuple<String, String>> list = new ArrayList<>();
            for (int i = 0; i < oacis.size(); i++) {
                for (int j = i + 1; j < oacis.size(); j++) {
                    list.add(new Tuple<>(oacis.get(i), oacis.get(j)));
                }
            }
            // ordenamos alfabeticamente los pares de oaci
            list.sort((t1, t2) -> {
                    if (t1.getaVal().compareTo(t2.getaVal()) == 0) { // primer oaci es igual entonces comparamos con el segundo
                        return t1.getbVal().compareTo(t2.getbVal());
                    } else {
                        return t1.getaVal().compareTo(t2.getaVal());
                    }
                }
            );
            return list;
        }
    }
}
