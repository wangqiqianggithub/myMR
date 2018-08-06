import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class MRdriver {

    public static void main(String[] args) throws OdpsException {

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString("area:String,goods:String"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("count:String,sum:double"));

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

        // TODO: specify a mapper
        job.setMapperClass(MRmapper.class);
        // TODO: specify a reducer
        job.setReducerClass(MRreduce.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}