package threads_perYear;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class HundredSixtyThreads {

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

            ExecutorService cityExecutor = Executors.newFixedThreadPool(160);
            List<Future<Map<String, Map<String, List<Double>>>>> futures = new ArrayList<>();

            long threadStartTime = System.nanoTime();

            List<String> cityList = new ArrayList<>(filesByCity.keySet());
            int totalCities = cityList.size();
            int citiesPerThread = 2;  // 2 cidades por thread

            for (int i = 0; i < 160; i++) {
                int start = i * citiesPerThread;
                int end = Math.min(start + citiesPerThread, totalCities);

                if (start >= totalCities) break;

                List<String> citySubList = cityList.subList(start, end);

                futures.add(cityExecutor.submit(() -> {
                    Map<String, Map<String, List<Double>>> yearTemperaturesByCity = new HashMap<>();

                    for (String city : citySubList) {
                        List<Path> cityFiles = filesByCity.get(city);
                        yearTemperaturesByCity.put(city, processCityFiles(city, cityFiles));
                    }

                    return yearTemperaturesByCity;
                }));
            }

            cityExecutor.shutdown();
            cityExecutor.awaitTermination(1, TimeUnit.HOURS);

            long threadEndTime = System.nanoTime();
            long threadDuration = threadEndTime - threadStartTime;

            Map<String, Map<String, List<Double>>> finalCityTemperatures = new HashMap<>();
            for (Future<Map<String, Map<String, List<Double>>>> future : futures) {
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

    private static Map<String, List<Double>> processCityFiles(String city, List<Path> cityFiles) throws InterruptedException, ExecutionException {
        ExecutorService yearExecutor = Executors.newCachedThreadPool();
        List<Future<Map.Entry<String, List<Double>>>> yearFutures = new ArrayList<>();

        for (Path csvFile : cityFiles) {
            yearFutures.add(yearExecutor.submit(() -> processYearFile(csvFile)));
        }

        yearExecutor.shutdown();
        yearExecutor.awaitTermination(1, TimeUnit.HOURS);

        Map<String, List<Double>> yearTemperatures = new HashMap<>();
        for (Future<Map.Entry<String, List<Double>>> future : yearFutures) {
            Map.Entry<String, List<Double>> yearEntry = future.get();
            yearTemperatures.put(yearEntry.getKey(), yearEntry.getValue());
        }

        return yearTemperatures;
    }

    private static Map.Entry<String, List<Double>> processYearFile(Path csvFile) throws IOException, CsvValidationException {
        String year = null;
        List<Double> temperatures = new ArrayList<>();

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
                year = date.split("-")[0];

                temperatures.add(temperature);
            }
        }

        return new AbstractMap.SimpleEntry<>(year, temperatures);
    }

    private static void calculateAndPrintTemperatures(Map<String, Map<String, List<Double>>> cityTemperatures) {
        for (Map.Entry<String, Map<String, List<Double>>> cityEntry : cityTemperatures.entrySet()) {
            String city = cityEntry.getKey();
            Map<String, List<Double>> yearData = cityEntry.getValue();

            System.out.println("Cidade: " + city);
            for (Map.Entry<String, List<Double>> yearEntry : yearData.entrySet()) {
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
}

