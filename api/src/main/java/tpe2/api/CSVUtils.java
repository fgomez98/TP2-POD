package tpe2.api;

import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
