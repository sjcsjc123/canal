package com.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class canalDemo {

    public static void main(String[] args) throws InvalidProtocolBufferException, InterruptedException {
        //创建链接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("linux1", 11111),
                "example", "", "");
        while (true) {
            //获取链接
            canalConnector.connect();
            //监控数据库
            canalConnector.subscribe("canal_test.*");
            //获取message
            Message message = canalConnector.get(100);
            //获取entry
            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() <= 0) {
                System.out.println("当次抓取没有数据");
                //每三秒抓一次，没有数据的话
                Thread.sleep(3000);
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //表名
                    String tableName = entry.getHeader().getTableName();
                    //操作类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (entryType.equals(CanalEntry.EntryType.ROWDATA)) {
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange =
                                CanalEntry.RowChange.parseFrom(storeValue);
                        //事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        List<CanalEntry.RowData> rowDatasList =
                                rowChange.getRowDatasList();
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            //获取执行sql语句之前的数据
                            JSONObject before = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList =
                                    rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                before.put(column.getName(), column.getValue());
                            }
                            //获取执行sql执行之后的数据
                            JSONObject after = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList =
                                    rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                after.put(column.getName(), column.getValue());
                            }
                            //数据打印
                            System.out.println("Table:" + tableName +
                                    ",EventType:" + eventType +
                                    ",Before:" + before +
                                    ",After:" + after);
                        }
                    }
                }
            }
        }
    }
}