package tpe2.api;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Tuple<K, V> implements DataSerializable {

    private K aVal;
    private V bVal;

    public Tuple(K aVal, V bVal) {
        this.aVal = aVal;
        this.bVal = bVal;
    }

    public K getaVal() {
        return aVal;
    }

    public void setaVal(K aVal) {
        this.aVal = aVal;
    }

    public V getbVal() {
        return bVal;
    }

    public void setbVal(V bVal) {
        this.bVal = bVal;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(aVal);
        out.writeObject(bVal);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        aVal = in.readObject();
        bVal = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (aVal != null ? !aVal.equals(tuple.aVal) : tuple.aVal != null) return false;
        return bVal != null ? bVal.equals(tuple.bVal) : tuple.bVal == null;
    }

    @Override
    public int hashCode() {
        int result = aVal != null ? aVal.hashCode() : 0;
        result = 31 * result + (bVal != null ? bVal.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return aVal + ", " + bVal;
    }
}
