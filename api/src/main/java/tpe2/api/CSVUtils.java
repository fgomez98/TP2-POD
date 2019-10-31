package tpe2.api;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CSVUtils {

    public static List<Airport> CSVReadAirports(String path) throws Exception {
        CSVReader reader = new CSVReader(new FileReader(path), ';');
        String [] nextLine = reader.readNext(); // salteamos al priemra
        List<Airport> resp = new ArrayList<>();
        while ((nextLine = reader.readNext()) != null) {
            resp.add(new Airport(nextLine[1], nextLine[4], nextLine[21]));
        }
        return resp;
    }

    public static List<Flight> CSVReadFlights(String path) throws Exception {
        CSVReader reader = new CSVReader(new FileReader(path), ';');
        String [] nextLine = reader.readNext(); // salteamos al priemra
        List<Flight> resp = new ArrayList<>();
        while ((nextLine = reader.readNext()) != null) {
            resp.add(new Flight(nextLine[2], nextLine[3], nextLine[4], nextLine[5], nextLine[6], nextLine[7]));
        }
        return resp;
    }

}
