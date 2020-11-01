package com.bugdbug.customsource.jdbc.read;

import com.bugdbug.customsource.jdbc.JdbcParams;
import com.bugdbug.customsource.jdbc.utils.JdbcUtil;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.sql.SQLException;
import java.util.List;

public class JdbcBatch implements Batch {
    private final StructType schema;
    private final JdbcParams jdbcParams;

    public JdbcBatch(StructType schema, JdbcParams jdbcParams) {
        this.schema = schema;
        this.jdbcParams = jdbcParams;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return createPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new JdbcPartitionReaderFactory(schema, jdbcParams);
    }

    private InputPartition[] createPartitions() {
        InputPartition[] partitions = new JdbcInputPartition[jdbcParams.getNumPartitions()];
        try {
            List<Integer> partitionsColumnValues = JdbcUtil.getPartitionsColumnValues(jdbcParams);
            int bucketSize = (int) Math.ceil(partitionsColumnValues.size() * 1.0 / jdbcParams.getNumPartitions());
            int startIndex = 0;
            int endIndex = bucketSize;
            int index = 0;
            String lastHostAddr=null, hostAddr;
            while (startIndex < partitionsColumnValues.size() && endIndex <= partitionsColumnValues.size()) {
                Integer[] values = partitionsColumnValues.subList(startIndex, endIndex).stream().toArray(Integer[]::new);
                hostAddr=getLocalityInfo(jdbcParams.getLocalityInfo(),lastHostAddr,index);
                partitions[index++] = new JdbcInputPartition(values,hostAddr);
                lastHostAddr=hostAddr;
                startIndex = endIndex;
                endIndex = Math.min((endIndex + bucketSize), partitionsColumnValues.size());
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return partitions;
    }

    private String getLocalityInfo(List<String> localityInfo, String lastHostAddr, int index) {
        if (localityInfo.size() - 1 < index)
            return lastHostAddr;
        else return localityInfo.get(index);
    }
}
