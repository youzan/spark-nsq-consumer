

import com.youzan.bigdata.streaming.nsq.NSQMessageWrapper;
import com.youzan.bigdata.streaming.utils.Utils;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQMessage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by chenjunzou on 2017/3/21.
 */
public class SerializeTest {
    public static void main(String [] args) throws Exception{
        String filename = "serial.bin";
        String msg = "{\n" +
                "  \"end_date\": \"\",\n" +
                "  \"add_mq_time\": 1507722531898,\n" +
                "  \"binlogTime\": 1507722531000,\n" +
                "  \"billing_event\": \"0\",\n" +
                "  \"time_series\": \"111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111\",\n" +
                "  \"total_budget\": \"0\",\n" +
                "  \"begin_date\": \"2017-10-11\",\n" +
                "  \"schemaName\": \"bigdata_dsp\",\n" +
                "  \"binlogOffset\": 635173186,\n" +
                "  \"binlogTableName\": \"platform_advertisement\",\n" +
                "  \"targeting_id\": \"21926868\",\n" +
                "  \"bid_amount\": \"89\",\n" +
                "  \"binlogApplyTime\": 1507722531897,\n" +
                "  \"id\": \"34372260\",\n" +
                "  \"a_name\": \"BLK\",\n" +
                "  \"campaign_id\": \"5442191\",\n" +
                "  \"audit_msg\": \"\",\n" +
                "  \"nsqTraceId\": 1507722531898,\n" +
                "  \"p_status\": \"1\",\n" +
                "  \"daily_budget\": \"0\",\n" +
                "  \"site_set\": \"SITE_SET_WECHAT\",\n" +
                "  \"binlogSequenceNo\": 1507722531734000000,\n" +
                "  \"eventType\": \"UPDATE\",\n" +
                "  \"binlogTransactionNo\": 150772253100028,\n" +
                "  \"a_type\": \"OFFICIAL\",\n" +
                "  \"binlogFileName\": \"mysql-bin.003874\",\n" +
                "  \"account_id\": \"5392600\",\n" +
                "  \"update_at\": \"2017-10-11 19:48:51\"\n" +
                "}";
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
        NSQMessage message = new NSQMessage(Utils.longToBytes(1), Utils.shortToBytes((short)2),
                Utils.longToBytes(3), Utils.longToBytes(4), Utils.longToBytes(5),
                msg.getBytes(), new Address("123","123", "123", "topic", 0), 6, 7);

        NSQMessageWrapper wrapper = new NSQMessageWrapper(message);
        out.writeObject(wrapper);

        ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
        wrapper = (NSQMessageWrapper) in.readObject();
        System.out.println(wrapper.getMessage().getReadableContent());

    }
}

