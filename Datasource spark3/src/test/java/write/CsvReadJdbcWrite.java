package write;

import com.bugdbug.customsource.jdbc.Constants;
import com.bugdbug.customsource.jdbc.JdbcParams;
import com.bugdbug.customsource.jdbc.JdbcSource;
import com.bugdbug.customsource.jdbc.utils.JdbcUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvReadJdbcWrite implements Runnable {

    private final SparkSession sparkSession;

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();
        CsvReadJdbcWrite runner = new CsvReadJdbcWrite(sparkSession);
        runner.run();
    }

    CsvReadJdbcWrite(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void run() {
        JdbcParams jdbcParams = null;
        try {
            Dataset<Row> dataset = readCSV();
            jdbcParams = preRunSetup(dataset.schema());
            writeToJdbc(dataset, jdbcParams);
        } finally {
            if (jdbcParams != null)
                JdbcUtil.dropTable(jdbcParams);
        }


    }

    private void writeToJdbc(Dataset<Row> dataset, JdbcParams jdbcParams) {

        DataFrameWriter<Row> dataFrameWriter = dataset
                .write()
                .format(JdbcSource.class.getCanonicalName())
                .option(Constants.TABLE_NAME, jdbcParams.getTableName())
                .option(Constants.JDBC_URL, jdbcParams.getJdbcUrl())
                .option(Constants.USER, jdbcParams.getUserName())
                .option(Constants.PASSWORD, jdbcParams.getPassword())
                .option(Constants.JDBC_DRIVER, jdbcParams.getJdbcDriver())
                .mode(SaveMode.Append);

        dataFrameWriter.save();
    }

    private Dataset<Row> readCSV() {
        Dataset<Row> dataset = sparkSession.read().schema(getSchema()).format("com.bugdbug.customsource.csv.CSV")
                .option("fileName", "1000 Sales Records.csv").load();
        dataset.show();
        return dataset;
    }

    private static StructType getSchema() {
        StructField[] structFields = new StructField[]{
                new StructField("Region", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Item Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Sales Channel", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Priority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Ship Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Units Sold", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Unit Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unit Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Revenue", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Profit", DataTypes.DoubleType, true, Metadata.empty())
        };
        return new StructType(structFields);
    }

    private JdbcParams buildJdbcParams() {
        return new JdbcParams.JdbcParamsBuilder()
                .setTableName("SALES_DATA")
                .setJdbcDriver("org.h2.Driver")
                .setJdbcUrl("jdbc:h2:~/test")
                .setPassword("")
                .setUserName("sa")
                .build();
    }

    private JdbcParams preRunSetup(StructType schema) {
        JdbcParams jdbcParams = buildJdbcParams();
        JdbcUtil.createTable(schema, jdbcParams);
        return jdbcParams;
    }
}

