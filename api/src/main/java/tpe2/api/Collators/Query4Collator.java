package tpe2.api.Collators;

import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Query4Collator implements Collator<Map.Entry<String, Long>, Map<String, Long>> {

    private int n;

    public Query4Collator(int n) {
        this.n = n;
    }

    @Override
    public Map<String, Long> collate(Iterable<Map.Entry<String, Long>> values) {
        List<Map.Entry<String, Long>> entryList = new ArrayList<>();
        values.forEach(entryList::add);
        entryList.sort((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()));

        return entryList.subList(0,n).stream().collect(Collectors.toMap(Map.Entry::getKey,
                Map.Entry::getValue,
                (v1,v2)->v1,
                LinkedHashMap::new));
    }
}

