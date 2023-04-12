package com.tzq.bus;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title 精确一次 示例  但是没有输出，暂时无解 ToDo
 * @Author zhengqiang.tan
 * @Date 4/9/23 5:28 PM
 */
public class JdbcSinkExactlyOneceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
                new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
                new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
                new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
        ).addSink(
                JdbcSink.exactlyOnceSink(
//                        "insert into t_books (id, title, authors, year) values (?, ?, ?, ?) on duplicate key update id = values(id)",
                        "insert into t_books (id, title, authors, year) values (?, ?, ?, ?)",
                        (statement, book) -> {
                            statement.setLong(1, book.id);
                            statement.setString(2, book.title);
                            statement.setString(3, book.authors);
                            statement.setInt(4, book.year);
                        },
                        JdbcExecutionOptions.builder()
                                .withMaxRetries(0)// 默认3 注：JDBC XA sink requires maxRetries equal to 0
//                                .withBatchSize(1) // 默认5000
                                .build(),
                        JdbcExactlyOnceOptions.builder()
                                .withTransactionPerConnection(true)
                                .build(),
                        () -> {
                            // create a driver-specific XA DataSource
                            // The following example is for mysql
                            MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
                            xaDataSource.setUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&serverTimezone=UTC");
                            xaDataSource.setUser("root");
                            xaDataSource.setPassword("root");
                            return xaDataSource;
                        })
        ).name("sink2mysql");


        env.execute("test sink to mysql");
    }

    static class Book {
        public Book(Long id, String title, String authors, Integer year) {
            this.id = id;
            this.title = title;
            this.authors = authors;
            this.year = year;
        }

        final Long id;
        final String title;
        final String authors;
        final Integer year;
    }
}