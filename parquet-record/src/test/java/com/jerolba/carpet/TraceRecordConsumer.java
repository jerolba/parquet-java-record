package com.jerolba.carpet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

public class TraceRecordConsumer extends RecordConsumer {

    private String tab = "";

    @Override
    public void startMessage() {
        System.out.println("Start message");
    }

    @Override
    public void endMessage() {
        System.out.println("End message");
    }

    @Override
    public void startField(String field, int index) {
        System.out.println(tab + "<" + field + ">");
    }

    @Override
    public void endField(String field, int index) {
        System.out.println(tab + "</" + field + ">");

    }

    @Override
    public void startGroup() {
        this.tab = tab + "\t";
    }

    @Override
    public void endGroup() {
        this.tab = tab.substring(0, tab.length() - 1);
    }

    @Override
    public void addInteger(int value) {
        System.out.println(tab + value);
    }

    @Override
    public void addLong(long value) {
        System.out.println(tab + value);
    }

    @Override
    public void addBoolean(boolean value) {
        System.out.println(tab + value);
    }

    @Override
    public void addBinary(Binary value) {
        System.out.println(tab + value);
    }

    @Override
    public void addFloat(float value) {
        System.out.println(tab + value);
    }

    @Override
    public void addDouble(double value) {
        System.out.println(tab + value);
    }

}
