package tpe2.api.Model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.opencsv.bean.CsvBindByPosition;

import java.io.IOException;

public class Airport implements DataSerializable {

    @CsvBindByPosition(position = 1)
    private String oaci;
    @CsvBindByPosition(position = 4)
    private String denomination;
    @CsvBindByPosition(position = 21)
    private String province;

    public Airport(String oaci, String denomination, String province) {
        this.oaci = oaci;
        this.denomination = denomination;
        this.province = province;
    }

    public String getOaci() {
        return oaci;
    }

    public void setOaci(String oaci) {
        this.oaci = oaci;
    }

    public String getDenomination() {
        return denomination;
    }

    public void setDenomination(String denomination) {
        this.denomination = denomination;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    // NO CAMBIAR EL ORDEN, DEBEN COINCIDIR EN READ Y WRITE
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(oaci);
        out.writeUTF(denomination);
        out.writeUTF(province);
    }

    // NO CAMBIAR EL ORDEN, DEBEN COINCIDIR EN READ Y WRITE
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oaci = in.readUTF();
        denomination = in.readUTF();
        province = in.readUTF();
    }

    @Override
    public String toString() {
        return "Airport{" +
                "oaci='" + oaci + '\'' +
                ", denomination='" + denomination + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
