package tpe2.api;

import com.opencsv.bean.CsvBindByPosition;

public class Airport {

    @CsvBindByPosition(position = 1)
    private String oaci;
    @CsvBindByPosition(position = 4)
    private String denomination;
    @CsvBindByPosition(position = 21)
    private String province;

    public Airport() {
    }

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

    @Override
    public String toString() {
        return "Airport{" +
                "oaci='" + oaci + '\'' +
                ", denomination='" + denomination + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
