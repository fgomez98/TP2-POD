package tpe2.api;

import com.opencsv.bean.CsvBindByPosition;

import java.io.Serializable;

public class Flight implements Serializable{

    @CsvBindByPosition(position = 2)
    private String flightClass;
    @CsvBindByPosition(position = 3)
    private String flightCLassification;
    @CsvBindByPosition(position = 4)
    private String typeOfMovement;
    @CsvBindByPosition(position = 5)
    private String oaciOrigin;
    @CsvBindByPosition(position = 6)
    private String oaciDestination;
    @CsvBindByPosition(position = 7)
    private String airline;

    public Flight(){
    }

    public Flight(String flightClass, String flightCLassification, String typeOfMovement, String oaciOrigin, String oaciDestination, String airline) {
        this.flightClass = flightClass;
        this.flightCLassification = flightCLassification;
        this.typeOfMovement = typeOfMovement;
        this.oaciOrigin = oaciOrigin;
        this.oaciDestination = oaciDestination;
        this.airline = airline;
    }

    public String getFlightClass() {
        return flightClass;
    }

    public void setFlightClass(String flightClass) {
        this.flightClass = flightClass;
    }

    public String getFlightCLassification() {
        return flightCLassification;
    }

    public void setFlightCLassification(String flightCLassification) {
        this.flightCLassification = flightCLassification;
    }

    public String getTypeOfMovement() {
        return typeOfMovement;
    }

    public void setTypeOfMovement(String typeOfMovement) {
        this.typeOfMovement = typeOfMovement;
    }

    public String getOaciOrigin() {
        return oaciOrigin;
    }

    public void setOaciOrigin(String oaciOrigin) {
        this.oaciOrigin = oaciOrigin;
    }

    public String getOaciDestination() {
        return oaciDestination;
    }

    public void setOaciDestination(String oaciDestination) {
        this.oaciDestination = oaciDestination;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    @Override
    public String toString() {
        return "Flight{" +
                "flightClass='" + flightClass + '\'' +
                ", flightCLassification='" + flightCLassification + '\'' +
                ", typeOfMovement='" + typeOfMovement + '\'' +
                ", oaciOrigin='" + oaciOrigin + '\'' +
                ", oaciDestination='" + oaciDestination + '\'' +
                ", airline='" + airline + '\'' +
                '}';
    }
}
