package tpe2.api.Collators;


import com.hazelcast.mapreduce.Collator;
import tpe2.api.Model.Tuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Query2Collator implements Collator<Map.Entry<String, Long>, List<Tuple<String, Double>>> {

    private int n;

    public Query2Collator(int n) {
        this.n = n;
    }

    @Override
    public List<Tuple<String, Double>> collate(Iterable<Map.Entry<String, Long>> values) {
        AtomicLong counter = new AtomicLong(0);
        List<Tuple<String, Double>> ret = new ArrayList<>();
        List<Tuple<String, Long>> list = new ArrayList<>();
        values.forEach((entry) -> {
            counter.addAndGet(entry.getValue());
            list.add(new Tuple<>(entry.getKey(),entry.getValue()));
        });
        list.sort((o1, o2) -> (int) (o2.getbVal() - o1.getbVal()));
        double pctg = 0;
        for (int i = 0; i < n && i < list.size(); i++) {
            if (!list.get(i).getaVal().equals("N/A")) {
                double value = 100.0 * list.get(i).getbVal() / counter.longValue();
                pctg += value;
                ret.add(new Tuple<>(list.get(i).getaVal(), value));
            }else{
                n+=1;
            }
        }
        ret.add(new Tuple<>("Otros", (100.0 - pctg) >= 0 ? 100.0 - pctg : 0));
        return ret;
    }
}
