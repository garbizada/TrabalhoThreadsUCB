package csvreader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {

    public static void main(String[] args) {
        try {
        	
            Path csvDirectory = Paths.get("C:\\Users\\Cauê Justen Garbi\\Downloads\\temperaturas_cidades\\temperaturas_cidades");
            
           
            List<Path> csvFiles = Files.walk(csvDirectory)
                                       .filter(Files::isRegularFile)
                                       .filter(p -> p.toString().endsWith(".csv"))
                                       .toList();

            for (Path csvFile : csvFiles) {
                readCsvFile(csvFile);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readCsvFile(Path csvFile) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile.toFile()))) {
            String[] nextLine;
            

            while ((nextLine = reader.readNext()) != null) {
                System.out.println("Lendo arquivo: " + csvFile.getFileName());
                for (String cell : nextLine) {
                    System.out.print(cell + " ");
                }
                System.out.println();
            }

        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }
}
