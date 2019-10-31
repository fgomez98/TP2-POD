package tpe2.api.Collators;


import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Query2Collator implements Collator<Map.Entry<String, Long>, Set<Map.Entry<String, Double>>> {

    private int n;

    public Query2Collator(int n) {
        this.n = n;
    }

    @Override
    public Set<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Long>> values) {
        AtomicLong counter = new AtomicLong(0);
        Map<String, Double> ret = new TreeMap<>();
        List<Map.Entry<String, Long>> list = new ArrayList<>();
        values.forEach((entry) -> {
            counter.addAndGet(entry.getValue());
            list.add(entry);
        });
        System.out.println(counter);
        list.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));
        double pctg = 0;
        for (int i = 0; i < n && i < list.size(); i++) {
            if (!list.get(i).getKey().equals("N/A")) {
                double value = 100.0 * list.get(i).getValue() / counter.longValue();
                pctg += value;
                ret.put(list.get(i).getKey(), value);
            }else{
                n+=1;
            }
        }
        ret.put("Otros", (100.0 - pctg) >= 0 ? 100.0 - pctg : 0);
        return ret.entrySet();
    }
}
