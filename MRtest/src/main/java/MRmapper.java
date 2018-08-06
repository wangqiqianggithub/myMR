import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class MRmapper extends MapperBase {

    Record key;
    Record value;

    @Override
    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        //TODO
        key.set("area",record.getString(1));
        key.set("goods",record.getString(2));

        value.set("count",record.getString(1));
        value.set("sum",record.getDouble(3));

        context.write(key,value);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}