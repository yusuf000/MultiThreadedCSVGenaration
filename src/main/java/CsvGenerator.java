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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvGenerator{

    File csvFile;
    List<CsvRow> csvRows = new ArrayList<>();
    Executor executor = Executors.newFixedThreadPool(20);


    public CsvGenerator(String filePath) throws IOException, CsvRequiredFieldEmptyException, CsvDataTypeMismatchException {
        boolean allThreadDone = false;
        List<CsvGenerate> generators = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            CsvGenerate csvGenerate = new CsvGenerate(i);
            executor.execute(csvGenerate);
        }

        File file = new File(filePath);
        FileWriter outputfile = new FileWriter(file);
        CSVWriter fileWriter = new CSVWriter(outputfile);
        StatefulBeanToCsv<CsvRow> writer = new StatefulBeanToCsvBuilder<CsvRow>(fileWriter)
                .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                .withOrderedResults(false)
                .build();

        //write all data to csv file
        writer.write(csvRows);
        fileWriter.close();
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
            for (int i = 0; i < 1000; i++) {
                rows.add(new CsvRow(this.threadNumber, random.nextInt(), random.nextInt()));
            }
            addToList(rows);
        }
        synchronized void addToList(List<CsvRow> rows) {
            csvRows.addAll(rows);
        }
    }

}
