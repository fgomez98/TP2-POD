import java.util.Objects;

public class Airport {

    private String local;
    private String oaci;
    private String iata;
    private String denomination;
    private String coordinates;
    private double latitude;
    private double longitude;
    private double elevation;
    private String elevationUnit;
    private String reference;
    private double distanceToReference;
    private String referenceDirection;
    private String condition;
    private String control;
    private String region;
    private String fir;
    private String use;
    private String traffic;
    private String sna;
    private String concession;
    private String province;
    private String inheb;


    public Airport(String local, String oaci, String iata, String denomination, String coordinates, double latitude, double longitude, double elevation, String elevationUnit, String reference, double distanceToReference, String referenceDirection, String condition, String control, String region, String fir, String use, String traffic, String sna, String concession, String province, String inheb) {
        this.local = local;
        this.oaci = oaci;
        this.iata = iata;
        this.denomination = denomination;
        this.coordinates = coordinates;
        this.latitude = latitude;
        this.longitude = longitude;
        this.elevation = elevation;
        this.elevationUnit = elevationUnit;
        this.reference = reference;
        this.distanceToReference = distanceToReference;
        this.referenceDirection = referenceDirection;
        this.condition = condition;
        this.control = control;
        this.region = region;
        this.fir = fir;
        this.use = use;
        this.traffic = traffic;
        this.sna = sna;
        this.concession = concession;
        this.province = province;
        this.inheb = inheb;
    }


    public String getLocal() {
        return local;
    }

    public void setLocal(String local) {
        this.local = local;
    }

    public String getOaci() {
        return oaci;
    }

    public void setOaci(String oaci) {
        this.oaci = oaci;
    }

    public String getIata() {
        return iata;
    }

    public void setIata(String iata) {
        this.iata = iata;
    }

    public String getDenomination() {
        return denomination;
    }

    public void setDenomination(String denomination) {
        this.denomination = denomination;
    }

    public String getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(String coordinates) {
        this.coordinates = coordinates;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getElevation() {
        return elevation;
    }

    public void setElevation(double elevation) {
        this.elevation = elevation;
    }

    public String getElevationUnit() {
        return elevationUnit;
    }

    public void setElevationUnit(String elevationUnit) {
        this.elevationUnit = elevationUnit;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public double getDistanceToReference() {
        return distanceToReference;
    }

    public void setDistanceToReference(double distanceToReference) {
        this.distanceToReference = distanceToReference;
    }

    public String getReferenceDirection() {
        return referenceDirection;
    }

    public void setReferenceDirection(String referenceDirection) {
        this.referenceDirection = referenceDirection;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getControl() {
        return control;
    }

    public void setControl(String control) {
        this.control = control;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getFir() {
        return fir;
    }

    public void setFir(String fir) {
        this.fir = fir;
    }

    public String getUse() {
        return use;
    }

    public void setUse(String use) {
        this.use = use;
    }

    public String getTraffic() {
        return traffic;
    }

    public void setTraffic(String traffic) {
        this.traffic = traffic;
    }

    public String getSna() {
        return sna;
    }

    public void setSna(String sna) {
        this.sna = sna;
    }

    public String getConcession() {
        return concession;
    }

    public void setConcession(String concession) {
        this.concession = concession;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getInheb() {
        return inheb;
    }

    public void setInheb(String inheb) {
        this.inheb = inheb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Airport airport = (Airport) o;
        return Double.compare(airport.latitude, latitude) == 0 &&
                Double.compare(airport.longitude, longitude) == 0 &&
                Double.compare(airport.elevation, elevation) == 0 &&
                Double.compare(airport.distanceToReference, distanceToReference) == 0 &&
                Objects.equals(local, airport.local) &&
                Objects.equals(oaci, airport.oaci) &&
                Objects.equals(iata, airport.iata) &&
                Objects.equals(denomination, airport.denomination) &&
                Objects.equals(coordinates, airport.coordinates) &&
                Objects.equals(elevationUnit, airport.elevationUnit) &&
                Objects.equals(reference, airport.reference) &&
                Objects.equals(referenceDirection, airport.referenceDirection) &&
                Objects.equals(condition, airport.condition) &&
                Objects.equals(control, airport.control) &&
                Objects.equals(region, airport.region) &&
                Objects.equals(fir, airport.fir) &&
                Objects.equals(use, airport.use) &&
                Objects.equals(traffic, airport.traffic) &&
                Objects.equals(sna, airport.sna) &&
                Objects.equals(concession, airport.concession) &&
                Objects.equals(province, airport.province) &&
                Objects.equals(inheb, airport.inheb);
    }

    @Override
    public int hashCode() {
        return Objects.hash(local, oaci, iata, denomination, coordinates, latitude, longitude, elevation, elevationUnit, reference, distanceToReference, referenceDirection, condition, control, region, fir, use, traffic, sna, concession, province, inheb);
    }
}
