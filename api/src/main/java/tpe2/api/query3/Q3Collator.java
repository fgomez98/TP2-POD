package tpe2.api.query3;

import com.hazelcast.mapreduce.Collator;
import tpe2.api.Tuple;

import java.util.*;

public class Q3Collator implements Collator<Map.Entry<Long, List<Tuple<String, String>>>, List<Map.Entry<Long, List<Tuple<String, String>>>>> {

    @Override
    public List<Map.Entry<Long, List<Tuple<String, String>>>> collate(Iterable<Map.Entry<Long, List<Tuple<String, String>>>> iterable) {
        List<Map.Entry<Long, List<Tuple<String, String>>>> entryList = new ArrayList<>();
        iterable.forEach(entryList::add);
        entryList.sort((e1, e2) -> Long.compare(e2.getKey(), e1.getKey()));
        return entryList;
    }
}
