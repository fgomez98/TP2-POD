package tpe2.api;

import com.opencsv.bean.*;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CSVUtils {

    public static List CSVRead(Path path, Class clazz) throws Exception {
        ColumnPositionMappingStrategy ms = new ColumnPositionMappingStrategy();
        ms.setType(clazz);

        Reader reader = Files.newBufferedReader(path);

        CsvToBean cb = new CsvToBeanBuilder<>(reader)
                .withSkipLines(1) // no leo los headers
                .withType(clazz)
                .withMappingStrategy(ms)
                .withSeparator(';')
                .build();

        List beanList = cb.parse();
        reader.close();
        return beanList;
    }
    public static void main(String[] args) {
        try {
            List<Airport> airports = CSVRead(Paths.get("/Users/fermingomez/Desktop/aeropuertos.csv"), Airport.class);
            System.out.println(airports);
            List<Flight> flights = CSVRead(Paths.get("/Users/fermingomez/Desktop/movimientos.csv"), Flight.class);
            System.out.println(flights);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
