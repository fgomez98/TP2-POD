package tpe2.api;

public enum Collections {
    airports("g8-airports"),
    flights("g8-flights");

    public String name;

    public String getName() {
        return name;
    }

    Collections(String name) {
        this.name = name;
    }
}
