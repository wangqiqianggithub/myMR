import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class MRreduce extends ReducerBase {
    Record output;

    @Override
    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Long count = 0L;
        double sum = 0.0;

        while (values.hasNext()) {
            Record val = values.next();
            //System.out.print(val.getColumnCount()+"\n");
            double amount = val.getDouble("sum");

            count+=1;
            sum += amount;

        }

        output.set(0,key.getString("area"));
        output.set(1,key.getString("goods"));
        output.set(2,count);
        output.set(3,sum);

        context.write(output);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}