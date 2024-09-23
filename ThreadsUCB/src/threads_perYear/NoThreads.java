package threads_perYear;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class NoThreads {

    public static void main(String[] args) {
        Path csvDirectory = Paths.get("C:\\\\Users\\\\Cauê Justen Garbi\\\\Documents\\\\temperaturas_cidades");

        long overallStartTime = System.nanoTime();

        try {
            List<Path> csvFiles = Files.walk(csvDirectory)
                                       .filter(Files::isRegularFile)
                                       .filter(p -> p.toString().endsWith(".csv"))
                                       .limit(320)
                                       .toList();

            Map<String, List<Path>> filesByYear = new HashMap<>();

            for (Path file : csvFiles) {
                String fileName = file.getFileName().toString();
                String year = fileName.substring(fileName.length() - 8, fileName.length() - 4); // Ex: "data2020.csv"
                filesByYear.computeIfAbsent(year, k -> new ArrayList<>()).add(file);
            }

            ExecutorService executor = Executors.newCachedThreadPool();
            List<Future<Map<String, Map<String, List<Double>>>> > futures = new ArrayList<>();

            long threadStartTime = System.nanoTime(); // Início da medição de tempo das threads

            for (Map.Entry<String, List<Path>> entry : filesByYear.entrySet()) {
                String year = entry.getKey();
                List<Path> yearFiles = entry.getValue();

                futures.add(executor.submit(() -> processCsvFiles(yearFiles)));
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            long threadEndTime = System.nanoTime(); // Fim da medição de tempo das threads
            long threadDuration = threadEndTime - threadStartTime; // Duração das threads em nanosegundos

            Map<String, Map<String, List<Double>>> allCityTemperatures = new HashMap<>();

            for (Future<Map<String, Map<String, List<Double>>>> future : futures) {
                allCityTemperatures.putAll(future.get());
            }

            calculateAndPrintTemperatures(allCityTemperatures);

            long overallEndTime = System.nanoTime(); // Fim da medição geral
            long overallDuration = overallEndTime - overallStartTime; // Duração total em nanosegundos

            System.out.printf("Tempo total de execução das threads: %.2f segundos%n", threadDuration / 1_000_000_000.0);
            System.out.printf("Tempo total de execução do experimento: %.2f segundos%n", overallDuration / 1_000_000_000.0);

        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Erro: " + e.getMessage());
        }
    }

    private static Map<String, Map<String, List<Double>>> processCsvFiles(List<Path> csvFiles) {
        Map<String, Map<String, List<Double>>> cityTemperatures = new HashMap<>();

        for (Path csvFile : csvFiles) {
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

            } catch (IOException e) {
                System.err.println("Erro ao ler o arquivo " + csvFile.getFileName() + ": " + e.getMessage());
            } catch (CsvValidationException e) {
                System.err.println("Erro de validação CSV no arquivo " + csvFile.getFileName() + ": " + e.getMessage());
            }
        }

        return cityTemperatures;
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
