package tp2.api;

import java.io.Serializable;
import java.util.Date;

public class Mov implements Serializable {
    Date date;
    int i;
    String regular;
    String internacional;
    String despegue;
    String origen;
    String destino;
    String american_airlines;
    String s;
    public Mov(Date date, int i, String regular, String internacional, String despegue, String saco, String kmia, String american_airlines, String s) {
        this.date = date;
        this.i = i;
        this.regular = regular;
        this.internacional = internacional;
        this.despegue = despegue;
        this.origen = saco;
        this.destino = kmia;
        this.american_airlines = american_airlines;
        this.s = s;
    }
}
