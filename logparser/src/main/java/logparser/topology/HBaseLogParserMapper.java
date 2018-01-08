package logparser.topology;

import java.lang.Long;
import java.util.List;
import org.apache.commons.lang.StringUtils;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.hbase.common.ColumnList;
import static org.apache.storm.hbase.common.Utils.*;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;


public class HBaseLogParserMapper implements HBaseMapper {

    //rowKey: ClusterName.UserName.JobName.TaskName
    @Override
    public byte[] rowKey(Tuple tuple) {
        Object TaskId = tuple.getValueByField("task");
        return toBytes(TaskId);
    }

    //column: OverOrUnder.Resource.Timestamp
    @Override
    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        String AbnormalPartitions = tuple.getStringByField("abn_pars");
        if (!StringUtils.isEmpty(AbnormalPartitions)) {
            Long timestamp = tuple.getLongByField("timestamp");
            String timestampStr = Long.toString(timestamp);
            cols.addColumn("Resource".getBytes(), ("Over.CPU." + timestamp).getBytes(), AbnormalPartitions.getBytes());
        }
        return cols;
    }
}
