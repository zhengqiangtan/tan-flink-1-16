package com.tzq.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;


public class MultiplePatternExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> events = env.fromElements(
                new Event("buy", 1),
                new Event("sell", 2),
                new Event("sell", 3),
                new Event("buy", 4),
                new Event("sell", 5),
                new Event("buy", 6)
        );

        // 设置键字段
        KeySelector<Event, String> keySelector = (KeySelector<Event, String>) Event::getType;

        // 配置第一个模式
        Pattern<Event, ?> firstPattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getType().equals("sell");
            }
        });

        // 配置第二个模式
        Pattern<Event, ?> secondPattern = Pattern.<Event>begin("middle").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getType().equals("sell");
            }
        }).next("end").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getType().equals("buy");
            }
        });

        // 单独匹配第一个模式
        PatternStream<Event> firstPatternStream = CEP.pattern(events.keyBy(keySelector), firstPattern);
        firstPatternStream.select(new PatternSelectFunction<Event, Object>() {



            @Override
            public Object select(Map<String, List<Event>> map) throws Exception {
                System.out.println("开始匹配第一个模式，匹配到的事件序列：");
                for (Map.Entry<String, List<Event>> entry: map.entrySet()) {
                    for (Event event : entry.getValue()) {
                        System.out.println(event.toString());
                    }
                }
                return new Object();
            }
        });

        // 单独匹配第二个模式
        PatternStream<Event> secondPatternStream = CEP.pattern(events.keyBy(keySelector), secondPattern);
        secondPatternStream.select(new PatternSelectFunction<Event, Object>() {
            @Override
            public Object select(Map<String, List<Event>> map) throws Exception {
                System.out.println("开始匹配第二个模式，匹配到的事件序列：");
                for (Map.Entry<String, List<Event>> entry: map.entrySet()) {
                    for (Event event : entry.getValue()) {
                        System.out.println(event.toString());
                    }
                }
                return new Object();
            }
        });

        // 同时配置多个模式
        Pattern<Event, ?> multiplePatterns = firstPattern.followedBy(String.valueOf(secondPattern));
        PatternStream<Event> multiplePatternStream = CEP.pattern(events.keyBy(keySelector), multiplePatterns);
        multiplePatternStream.select(new PatternSelectFunction<Event, Object>() {
//            @Override
//            public Object timeout(Map<String, List<Event>> map, long l) throws Exception {
//                System.out.println("同时匹配多个模式，超时事件：");
//                for (Map.Entry<String, List<Event>> entry: map.entrySet()) {
//                    for (Event event : entry.getValue()) {
//                        System.out.println(event.toString());
//                    }
//                }
//                return new Object();
//            }
            @Override
            public Object select(Map<String, List<Event>> map) throws Exception {
                System.out.println("同时匹配多个模式，匹配到的事件序列：");
                for (Map.Entry<String, List<Event>> entry: map.entrySet()) {
                    for (Event event : entry.getValue()) {
                        System.out.println(event.toString());
                    }
                }
                return new Object();
            }
        });

        env.execute("Multiple Pattern Example");
    }

    public static class Event {
        private String type;
        private int id;

        public Event(String type, int id) {
            this.type = type;
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type='" + type + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}