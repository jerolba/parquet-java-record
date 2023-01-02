package com.jerolba.carpet;

import org.apache.parquet.io.api.RecordConsumer;

public class CarpetMessageWriter<T> {

    private final RecordConsumer recordConsumer;
    private final Class<T> recordClass;
    private final CarpetRecordWriter writer;

    public CarpetMessageWriter(RecordConsumer recordConsumer, Class<T> recordClass) throws Throwable {
        this.recordConsumer = recordConsumer;
        this.recordClass = recordClass;
        this.writer = new CarpetRecordWriter(recordConsumer, recordClass);
    }

    public void write(T record) {
        recordConsumer.startMessage();
        writer.write(record);
        recordConsumer.endMessage();
    }

}
