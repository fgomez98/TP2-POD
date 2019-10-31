package tpe2.api.query5;

import com.hazelcast.mapreduce.Collator;
import tpe2.api.Tuple;

import java.util.List;
import java.util.Map;

public class Q5Collator implements Collator<Map.Entry<String, Tuple<Long,Long>>, List<Map.Entry<String, Double>>> {

    private int n;

    public Q5Collator(int n) {
        this.n = n;
    }

    // la llave es el oaci
    //  privateCount == tuple.getaVal()
    // totalCount == tuple.getbVal()
    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Tuple<Long, Long>>> iterable) {
        // a tener en cuenta Todos los porcentajes deben imprimirse con dos dígitos decimales (truncados).
        // El orden de impresión es ascendente por porcentaje y luego alfabéticamente por código OACI.
        // Se deben listar únicamente las  n primeras líneas.
        return null;
    }
}
