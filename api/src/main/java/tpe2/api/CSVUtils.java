package tpe2.api;

import com.opencsv.CSVReader;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

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

    public static<K> void CSVWrite(Path path, Collection<K> data, String headers, Function<K, String> toCsvRow) throws IOException {
        Writer writer = new FileWriter(path.toString());
        writer.write(headers);
        for (K row : data) {
            writer.write(toCsvRow.apply(row));
        }
        writer.close();
    }

    public static<K> void CSVWrite(String path, Collection<K> data, String headers, Function<K, String> toCsvRow) throws IOException {
        CSVWrite(Paths.get(path), data, headers, toCsvRow);
    }

}
