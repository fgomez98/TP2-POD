package tpe2.api.query5;

import com.hazelcast.mapreduce.Collator;
import tpe2.api.Model.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Q5Collator implements Collator<Map.Entry<String, Tuple<Long, Long>>, List<Tuple<String, Double>>> {

    private int n;
    private List<String> oacis;

    public Q5Collator(List<String> oacis, int n) {
        this.n = n;
        this.oacis = oacis;
    }

    // la llave es el oaci
    //  privateCount == tuple.getaVal()
    // totalCount == tuple.getbVal()
    @Override
    public List<Tuple<String, Double>> collate(Iterable<Map.Entry<String, Tuple<Long, Long>>> values) {
        // a tener en cuenta Todos los porcentajes deben imprimirse con dos dígitos decimales (truncados).
        // El orden de impresión es ascendente por porcentaje y luego alfabéticamente por código OACI.
        // Se deben listar únicamente las  n primeras líneas.

        AtomicLong counter = new AtomicLong(0);
        for (Map.Entry<String, Tuple<Long, Long>> entry: values) {
            counter.addAndGet(entry.getValue().getbVal());
        }

        List<Tuple<String, Double>> ret = new ArrayList<>();

        values.forEach((entry) -> ret.add(new Tuple<>(entry.getKey(),100.0*(entry.getValue().getaVal())/counter.doubleValue())));

        Comparator<Tuple<String, Double>> a = (o1,o2)-> (int) (o1.getbVal()-o2.getbVal());
        Comparator<Tuple<String, Double>> aRev = (o1,o2)-> (int) (o2.getbVal()-o1.getbVal());

        return ret.stream()
                .filter((oaci) -> oacis.contains(oaci.getaVal()))
                .sorted(aRev.thenComparing((o1,o2)-> o2.getaVal().compareTo(o1.getaVal())))
                .limit(n)
                .sorted(a.thenComparing(Tuple::getaVal))
                .collect(Collectors.toList());
        //ret.sort(a.thenComparing(Tuple::getaVal));
        //return ret.subList(0,n<ret.size()?ret.size():n);
    }
}
