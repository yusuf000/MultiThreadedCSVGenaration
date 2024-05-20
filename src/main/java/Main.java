import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Main {
    public static void main(String[] args) {
        try {
            List<Future<File>> futureFiles = new ArrayList<>();
            List<CsvGenerator> csvGenerators = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                CsvGenerator csvGenerator = new CsvGenerator("Test_" + i + ".csv");
                futureFiles.add(csvGenerator.generateCsv());
                csvGenerators.add(csvGenerator);
            }
            for (int i = 0; i < 3; i++) {
                while(!futureFiles.get(i).isDone()) {
                    System.out.println("generating...");
                    Thread.sleep(300);
                }
            }

            System.out.println("job done");
            List<File> files = new ArrayList<>();
            for (Future<File> f: futureFiles) {
                files.add(f.get());
            }
            for (CsvGenerator c: csvGenerators ) {
                c.shutDown();
            }
            makeZip(files);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (CsvRequiredFieldEmptyException e) {
            throw new RuntimeException(e);
        } catch (CsvDataTypeMismatchException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Closed");

    }

    public static void makeZip(List<File> files) throws IOException {
        String file1 = "src/main/resources/zipTest/test1.txt";
        String file2 = "src/main/resources/zipTest/test2.txt";
        final List<String> srcFiles = Arrays.asList(file1, file2);

        final FileOutputStream fos = new FileOutputStream("compressed.zip");
        ZipOutputStream zipOut = new ZipOutputStream(fos);

        for (File fileToZip : files) {
            FileInputStream fis = new FileInputStream(fileToZip);
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);

            byte[] bytes = new byte[1024];
            int length;
            while((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fis.close();
        }

        zipOut.close();
        fos.close();
    }
}