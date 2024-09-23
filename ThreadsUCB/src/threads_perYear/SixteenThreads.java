package threads_perYear;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class SixteenThreads {

    public static void main(String[] args) {
        Path csvDirectory = Paths.get("C:\\\\Users\\\\Cauê Justen Garbi\\\\Documents\\\\temperaturas_cidades");

        long overallStartTime = System.nanoTime();

        try {
            List<Path> csvFiles = Files.walk(csvDirectory)
                                       .filter(Files::isRegularFile)
                                       .filter(p -> p.toString().endsWith(".csv"))
                                       .limit(320)
                                       .toList();

            Map<String, List<Path>> filesByCity = new HashMap<>();

            for (Path file : csvFiles) {
                String fileName = file.getFileName().toString();
                String city = fileName.substring(0, fileName.indexOf('_'));
                filesByCity.computeIfAbsent(city, k -> new ArrayList<>()).add(file);
            }

            ExecutorService cityExecutor = Executors.newFixedThreadPool(16);
            List<Future<Map<String, List<Double>>>> futures = new ArrayList<>();

            long threadStartTime = System.nanoTime();

            List<String> cityList = new ArrayList<>(filesByCity.keySet());
            int totalCities = cityList.size();
            int citiesPerThread = 20;  // 20 cidades por thread

            for (int i = 0; i < 16; i++) {
                int start = i * citiesPerThread;
                int end = Math.min(start + citiesPerThread, totalCities);

                if (start >= totalCities) break;

                List<String> citySubList = cityList.subList(start, end);

                futures.add(cityExecutor.submit(() -> {
                    Map<String, List<Double>> yearTemperatures = new HashMap<>();

                    for (String city : citySubList) {
                        List<Path> cityFiles = filesByCity.get(city);
                        yearTemperatures.putAll(processCityFiles(city, cityFiles));
                    }

                    return yearTemperatures;
                }));
            }

            cityExecutor.shutdown();
            cityExecutor.awaitTermination(1, TimeUnit.HOURS);

            long threadEndTime = System.nanoTime();
            long threadDuration = threadEndTime - threadStartTime;

            Map<String, List<Double>> finalCityTemperatures = new HashMap<>();
            for (Future<Map<String, List<Double>>> future : futures) {
                finalCityTemperatures.putAll(future.get());
            }

            calculateAndPrintTemperatures(finalCityTemperatures);

            long overallEndTime = System.nanoTime();
            long overallDuration = overallEndTime - overallStartTime;

            System.out.printf("Tempo total de execução das threads: %.2f segundos%n", threadDuration / 1_000_000_000.0);
            System.out.printf("Tempo total de execução do experimento: %.2f segundos%n", overallDuration / 1_000_000_000.0);

        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Erro: " + e.getMessage());
        }
    }

    private static Map<String, List<Double>> processCityFiles(String city, List<Path> cityFiles) {
        Map<String, List<Double>> yearTemperatures = new HashMap<>();

        for (Path csvFile : cityFiles) {
            try (CSVReader reader = new CSVReader(new FileReader(csvFile.toFile()))) {
                String[] nextLine;
                boolean isFirstLine = true;

                while ((nextLine = reader.readNext()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }

                    String date = nextLine[1];
                    double temperature = Double.parseDouble(nextLine[2]);
                    String year = date.split("-")[0];

                    yearTemperatures.computeIfAbsent(year, k -> new ArrayList<>()).add(temperature);
                }

            } catch (IOException | CsvValidationException e) {
                System.err.println("Erro ao ler o arquivo " + csvFile.getFileName() + ": " + e.getMessage());
            }
        }

        return yearTemperatures;
    }

    private static void calculateAndPrintTemperatures(Map<String, List<Double>> cityTemperatures) {
        for (Map.Entry<String, List<Double>> yearEntry : cityTemperatures.entrySet()) {
            String year = yearEntry.getKey();
            List<Double> temperatures = yearEntry.getValue();

            double sum = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;

            for (double temp : temperatures) {
                sum += temp;
                if (temp > max) max = temp;
                if (temp < min) min = temp;
            }

            double average = sum / temperatures.size();

            System.out.printf("Ano: %s, Média: %.2f, Máxima: %.2f, Mínima: %.2f%n",
                    year, average, max, min);
        }
    }
}

