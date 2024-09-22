package threads;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class noThreads {

    public static void main(String[] args) {
        Path csvDirectory = Paths.get("C:\\Users\\Cauê Justen Garbi\\Documents\\temperaturas_cidades");

        long startTime = System.nanoTime(); 

        try {
            List<Path> csvFiles = Files.walk(csvDirectory)
                                       .filter(Files::isRegularFile)
                                       .filter(p -> p.toString().endsWith(".csv"))
                                       .limit(320)
                                       .toList();

            for (Path csvFile : csvFiles) {
                processCsvFile(csvFile);
            }

        } catch (IOException e) {
            System.err.println("Erro ao acessar o diretório: " + e.getMessage());
        }

        long endTime = System.nanoTime(); 
        long duration = endTime - startTime; 
        System.out.printf("Tempo total de execução: %.2f segundos%n", duration / 1_000_000_000.0);
    }

    private static void processCsvFile(Path csvFile) {
        Map<String, Map<String, List<Double>>> cityTemperatures = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvFile.toFile()))) {
            String[] nextLine;
            boolean isFirstLine = true;

            while ((nextLine = reader.readNext()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                String city = nextLine[0];
                String date = nextLine[1];
                double temperature = Double.parseDouble(nextLine[2]);

                cityTemperatures
                    .computeIfAbsent(city, k -> new HashMap<>())
                    .computeIfAbsent(date, k -> new ArrayList<>())
                    .add(temperature);
            }

            calculateAndPrintTemperatures(cityTemperatures);

        } catch (IOException e) {
            System.err.println("Erro ao ler o arquivo " + csvFile.getFileName() + ": " + e.getMessage());
        } catch (CsvValidationException e) {
            System.err.println("Erro de validação CSV no arquivo " + csvFile.getFileName() + ": " + e.getMessage());
        }
    }

    private static void calculateAndPrintTemperatures(Map<String, Map<String, List<Double>>> cityTemperatures) {
        for (Map.Entry<String, Map<String, List<Double>>> cityEntry : cityTemperatures.entrySet()) {
            String city = cityEntry.getKey();
            Map<String, List<Double>> monthlyData = cityEntry.getValue();

            for (Map.Entry<String, List<Double>> monthEntry : monthlyData.entrySet()) {
                String month = monthEntry.getKey();
                List<Double> temperatures = monthEntry.getValue();

                double sum = 0;
                double max = Double.MIN_VALUE;
                double min = Double.MAX_VALUE;

                for (double temp : temperatures) {
                    sum += temp;
                    if (temp > max) max = temp;
                    if (temp < min) min = temp;
                }

                double average = sum / temperatures.size();

                System.out.printf("Cidade: %s, Mês: %s, Média: %.2f, Máxima: %.2f, Mínima: %.2f%n",
                        city, month, average, max, min);
            }
        }
    }
}



