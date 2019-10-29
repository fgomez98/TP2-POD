import java.util.Date;
import java.util.Objects;

public class Flight {

    private Date flightDate;
    private int time;
    private String flightClass;
    private String flightCLassification;
    private String typeOfMovement;
    private String oaciOrigin;
    private String oaciDestination;
    private String airline;
    private String airplane;

    public Flight(Date flightDate, int time, String flightClass, String flightCLassification, String typeOfMovement, String oaciOrigin, String oaciDestination, String airline, String airplane) {
        this.flightDate = flightDate;
        this.time = time;
        this.flightClass = flightClass;
        this.flightCLassification = flightCLassification;
        this.typeOfMovement = typeOfMovement;
        this.oaciOrigin = oaciOrigin;
        this.oaciDestination = oaciDestination;
        this.airline = airline;
        this.airplane = airplane;
    }

    public Date getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(Date flightDate) {
        this.flightDate = flightDate;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
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

    public String getAirplane() {
        return airplane;
    }

    public void setAirplane(String airplane) {
        this.airplane = airplane;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Flight flight = (Flight) o;
        return time == flight.time &&
                Objects.equals(flightDate, flight.flightDate) &&
                Objects.equals(flightClass, flight.flightClass) &&
                Objects.equals(flightCLassification, flight.flightCLassification) &&
                Objects.equals(typeOfMovement, flight.typeOfMovement) &&
                Objects.equals(oaciOrigin, flight.oaciOrigin) &&
                Objects.equals(oaciDestination, flight.oaciDestination) &&
                Objects.equals(airline, flight.airline) &&
                Objects.equals(airplane, flight.airplane);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flightDate, time, flightClass, flightCLassification, typeOfMovement, oaciOrigin, oaciDestination, airline, airplane);
    }

}
