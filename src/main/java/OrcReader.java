import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

public class OrcReader {
    public static void readToConsole(String filePath) throws java.io.IOException {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(filePath),
                OrcFile.readerOptions(conf));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        while (rows.nextBatch(batch)) {
            BytesColumnVector orgIDVector = (BytesColumnVector) batch.cols[0];
            BytesColumnVector userIDVector = (BytesColumnVector) batch.cols[1];
            BytesColumnVector userNameVector = (BytesColumnVector) batch.cols[2];
            BytesColumnVector IDVector = (BytesColumnVector) batch.cols[3];


            for(int r=0; r < batch.size; r++) {
                String orgID = new String(orgIDVector.vector[r], orgIDVector.start[r], orgIDVector.length[r]);
                String userID = new String(userIDVector.vector[r], userIDVector.start[r], userIDVector.length[r]);
                String userName = new String(userNameVector.vector[r], userNameVector.start[r], userNameVector.length[r]);
                String id = new String(IDVector.vector[r], IDVector.start[r], IDVector.length[r]);

                System.out.println(orgID + ", " + userID + ", " + userName + ", " + id);

            }
        }
        rows.close();
    }

    public static void main(String [ ] args) throws java.io.IOException {

    }
}