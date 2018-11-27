import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class OrcWriter {
    //test data
    final static String[] organizationIds = {"123", "124", "125"};
    final static String[] userIds = {"13436", "23456", "34566"};
    final static String[] userNames = {"Jim", "Jam", "Jom", "Job", "Johnson", "Jimmithy", "Jobinath", "Josherman", "Jackston"};
    final static String[] ids = {"11-1", "11-2", "11-3"};
    final static String defaultOutputPath = "test/output/";

    final static int numRowsTestFile = 100;

    private static void deleteOutputFolder(String outputFolderPath) throws java.io.IOException {
        //delete output folder
        File outputFolder = new File(outputFolderPath);
        if(outputFolder.isDirectory()) {
            System.out.println("Output folder found.");
            FileUtils.deleteDirectory(outputFolder);
            System.out.println(outputFolderPath + " deleted");
        }
    }
    public static ArrayList<String> createFilesFromRandom(String outputFolderPath, int numFiles) throws IOException {
        deleteOutputFolder(outputFolderPath);

        ArrayList<String> outputFilePaths = new ArrayList<String>();

        for (int fileNumber = 0; fileNumber < numFiles; fileNumber++ ) {

            final String outputFilePath = outputFolderPath + "orc-test-data-" + fileNumber + ".orc";
            Configuration conf = new Configuration();
            TypeDescription schema = AuditSchema.getSchema();
            Writer writer = OrcFile.createWriter(new Path(outputFilePath),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema));

            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector orgIDVector = (BytesColumnVector) batch.cols[0];
            BytesColumnVector userIDVector = (BytesColumnVector) batch.cols[1];
            BytesColumnVector userNameVector = (BytesColumnVector) batch.cols[2];
            BytesColumnVector IDVector = (BytesColumnVector) batch.cols[3];

            Random rand = new Random();
            for (int r = 0; r < numRowsTestFile; ++r) {
                int row = batch.size++;
                String orgId = UUID.randomUUID().toString();
                String userId = UUID.randomUUID().toString();
                String userName = userNames[new Random().nextInt(userNames.length)];
                String id = UUID.randomUUID().toString();

                orgIDVector.setVal(row, orgId.getBytes());
                userIDVector.setVal(row, userId.getBytes());
                userNameVector.setVal(row, userName.getBytes());
                IDVector.setVal(row, id.getBytes());

                // If the batch is full, write it out and start over.
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
                if (batch.size != 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            writer.close();
            outputFilePaths.add(outputFilePath);
        }
        return outputFilePaths;

    }

    public static ArrayList<String> createFilesFromTestData() throws IOException {
        return createFilesFromTestData(defaultOutputPath);
    }

    public static ArrayList<String> createFilesFromTestData(String outputFolderPath) throws IOException {
        ArrayList<String> outputFilePaths = new ArrayList<String>();

        deleteOutputFolder(outputFolderPath);

        // create test files
        for (int fileNumber = 0; fileNumber < 3; fileNumber++ ) {

            final String outputFilePath = outputFolderPath + "orc-test-data-" + fileNumber + ".orc";
            Configuration conf = new Configuration();
            TypeDescription schema = AuditSchema.getSchema();

            Writer writer = OrcFile.createWriter(new Path(outputFilePath),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema));

            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector orgIDVector = (BytesColumnVector) batch.cols[0];
            BytesColumnVector userIDVector = (BytesColumnVector) batch.cols[1];
            BytesColumnVector userNameVector = (BytesColumnVector) batch.cols[2];
            BytesColumnVector IDVector = (BytesColumnVector) batch.cols[3];


            int r = fileNumber;
            int row = batch.size++;
            orgIDVector.setVal(row, organizationIds[r].getBytes());
            userIDVector.setVal(row, userIds[r].getBytes());
            userNameVector.setVal(row, userNames[r].getBytes());
            IDVector.setVal(row, ids[r].getBytes());

            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
            outputFilePaths.add(outputFilePath);
        }
        return outputFilePaths;
    }

    public static void main(String[] args) throws IOException {
        createFilesFromTestData();
    }
}
