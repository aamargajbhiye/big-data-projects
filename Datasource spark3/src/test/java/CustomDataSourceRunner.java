import org.apache.spark.sql.SparkSession;

public class CustomDataSourceRunner {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();

        JdbcDataSourceRunner jdbcDataSourceRunner = new JdbcDataSourceRunner(sparkSession);
        jdbcDataSourceRunner.run();

        CsvDataSourceRunner csvDataSourceRunner = new CsvDataSourceRunner(sparkSession);
        csvDataSourceRunner.run();

    }
}
