package tpe2.api.Collators;

import com.hazelcast.mapreduce.Collator;
import tpe2.api.Model.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Query5Collator implements Collator<Map.Entry<String, Double>, List<Tuple<String, Double>>> {

    private int n;

    public Query5Collator(int n) {
        this.n = n;
    }

    @Override
    public List<Tuple<String, Double>> collate(Iterable<Map.Entry<String, Double>> values) {
        List<Tuple<String, Double>> ret = new ArrayList<>();
        values.forEach(e -> ret.add(new Tuple<>(e.getKey(), e.getValue())));

        Comparator<Tuple<String, Double>> percentageCmp = Comparator.comparingDouble(Tuple::getbVal);
        Comparator<Tuple<String, Double>> alfabeticCmp = Comparator.comparing(Tuple::getaVal);

        return ret.stream()
                .sorted(percentageCmp.thenComparing(alfabeticCmp))
                .limit(n)
                .sorted(percentageCmp.thenComparing(alfabeticCmp)).collect(Collectors.toList());
    }
}
