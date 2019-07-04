import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SparkIgfsDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("fs.igfs.impl", "org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem");
        sparkConf.setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        List<String> names = new ArrayList<>();
        names.add("value1");
        names.add("value2");
        Dataset<Row> dataFrame = sparkSession.createDataset(names, Encoders.STRING()).toDF();

        dataFrame.write().text("igfs://myFileSystem@192.168.0.106/file/file.txt");

        sparkSession.read().text("igfs://myFileSystem@192.168.0.106/file/file.txt").show();

    }
}
