import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;

import java.io.File;
import java.io.FileFilter;

public class OrcConsolidator {

    public static String consolidate(String folderPath) throws java.io.IOException {

        //Find Orc Files
        File directory = new File(folderPath);
        File[] orcFiles = directory.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                //get only orc files but ignore hidden files
                return pathname.getName().charAt(0) != '.' && pathname.getName().contains(".orc");
            }
        });

        //Setup Writer
        final String consolidatedFilePath = folderPath + "consolidated-file.orc";
        Configuration conf = new Configuration();
        TypeDescription schema = AuditSchema.getSchema();

        Writer writer = OrcFile.createWriter(new Path(consolidatedFilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));

        for (File orcFile : orcFiles) {
            Reader reader = OrcFile.createReader(new Path(folderPath + orcFile.getName()),
                    OrcFile.readerOptions(conf));

            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();

            while (rows.nextBatch(batch)) {
                writer.addRowBatch(batch);
            }
            rows.close();
        }
        writer.close();
        return consolidatedFilePath;
    }

    public static void main(String [ ] args) throws java.io.IOException {

    }

}
