import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class CsvGenerator{

    File csvFile;
    List<CsvRow> csvRows = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(20);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    CountDownLatch countDownLatch;


    public CsvGenerator(String filePath) {
        countDownLatch = new CountDownLatch(10);
        csvFile = new File(filePath);
    }

    public Future<File> generateCsv()  throws IOException, CsvRequiredFieldEmptyException, CsvDataTypeMismatchException, InterruptedException{
        return executorService.submit(() -> {
            List<CsvGenerate> generators = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                CsvGenerate csvGenerate = new CsvGenerate(i);
                executor.execute(csvGenerate);
            }
            countDownLatch.await();
            executor.shutdown();
            FileWriter outputfile = new FileWriter(csvFile);
            CSVWriter fileWriter = new CSVWriter(outputfile);
            StatefulBeanToCsv<CsvRow> writer = new StatefulBeanToCsvBuilder<CsvRow>(fileWriter)
                    .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                    .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                    .withOrderedResults(false)
                    .build();

            //write all data to csv file
            writer.write(csvRows);
            fileWriter.close();
            return csvFile;
        });
    }

    public void shutDown() {
        executorService.shutdown();
    }

    class CsvGenerate implements Runnable  {

        int threadNumber;
        Random random;
        public CsvGenerate(int tn) {
            random = new Random();
            this.threadNumber = tn;
        }
        @Override
        public void run() {
            List<CsvRow> rows = new ArrayList<>();
            System.out.println("Thread " + threadNumber);
            for (int i = 0; i < 10; i++) {
                rows.add(new CsvRow(this.threadNumber, random.nextInt(), random.nextInt()));
            }
            addToList(rows);
        }
        synchronized void addToList(List<CsvRow> rows) {
            csvRows.addAll(rows);
            System.out.println("Thread completed " + threadNumber);
            countDownLatch.countDown();
        }
    }

}
