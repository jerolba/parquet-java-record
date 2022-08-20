package com.jerolba.parquet.record;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class SimpleOutputStreamOutputFile implements OutputFile {

    private OutputStream outputStream;

    public SimpleOutputStreamOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new CountedPositionOutputStream(outputStream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new CountedPositionOutputStream(outputStream);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    private class CountedPositionOutputStream extends PositionOutputStream {

        private final BufferedOutputStream bos;
        private int pos = 0;

        public CountedPositionOutputStream(OutputStream os) {
            this.bos = new BufferedOutputStream(os);
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void flush() throws IOException {
            bos.flush();
        };

        @Override
        public void close() throws IOException {
            bos.close();
        };

        @Override
        public void write(int b) throws IOException {
            bos.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            bos.write(b, off, len);
            pos += len;
        }

        @Override
        public void write(byte[] b) throws IOException {
            bos.write(b);
            pos += b.length;
        }
    }

}
