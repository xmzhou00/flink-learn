package com.flinkcore.eos;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * eos 临时表抽象
 */
public class TransactionTable implements Serializable {
    private transient MyTransactionDB db;
    private final String transactionId;
    private final List<Tuple3<String, Long, String>> buffer = new ArrayList<>();

    public TransactionTable(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TransactionTable insert(Tuple3<String, Long, String> value) {
        initDB();
        buffer.add(value);
        return this;
    }

    public TransactionTable flush() {
        initDB();
        return null;
    }

    public void close() {
        buffer.clear();
    }

    private void initDB() {
        if (null == db) {
//            db = MyTransactionDB
        }
    }
}
