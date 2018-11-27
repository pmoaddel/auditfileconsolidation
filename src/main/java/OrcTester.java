import java.io.*;
import java.util.ArrayList;

public class OrcTester {

    private static final String testFileDirectory = "test/output/";

    public static void main(String[] args) throws IOException {
        System.out.println("Creating test files");
        ArrayList<String> TestFileNames = OrcWriter.createFilesFromRandom(testFileDirectory, 3);
        System.out.println("Files created");
        for (String path : TestFileNames) {
            System.out.println(path);
            System.out.println("File size: " + new File(path).length());
//            OrcReader.readToConsole(path);
        }
        System.out.println("Consolidating files");
        String consolidatedFileName = OrcConsolidator.consolidate(testFileDirectory);
        System.out.println("Files consolidated");
        System.out.println(consolidatedFileName);
        System.out.println("File size: " + new File(consolidatedFileName).length());
//        OrcReader.readToConsole(consolidatedFileName);
    }
}
