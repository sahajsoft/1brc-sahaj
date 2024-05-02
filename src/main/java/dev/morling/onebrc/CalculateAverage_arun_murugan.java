/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class CalculateAverage_arun_murugan {

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "/Volumes/RAM Disk/measurements_1B.txt";

    // private static record Aggregate(double min, double max, long count, double sum) {
    // public static Aggregate defaultAggr = new Aggregate(Double.POSITIVE_INFINITY,
    // Double.NEGATIVE_INFINITY, 0L, 0D);
    //
    // public Aggregate withUpdated(Double val) {
    // return new Aggregate(Math.min(min, val), Math.max(max, val), count + 1, sum + val);
    // }
    //
    // public Aggregate merge(Aggregate other) {
    // return new Aggregate(Math.min(min, other.min), Math.max(max, other.max), count + other.count, sum + other.sum);
    // }
    //
    // public String toString() {
    // return round(min) + "/" + round(round(sum) / count) + "/" + round(max);
    // }
    //
    // private double round(double value) {
    // return Math.round(value * 10.0) / 10.0;
    // }
    // }

    public static class Aggregate {
        private double min;
        private double max;
        private long count;
        private double sum;

        public static Aggregate getDefaultAggr() {
            return new Aggregate(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0L, 0D);
        }

        public Aggregate(double min, double max, long count, double sum) {
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
        }

        public Aggregate withUpdated(float val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            count++;
            sum += val;
            return this;
        }

        public Aggregate merge(Aggregate other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            count += other.count;
            sum += other.sum;
            return this;
        }

        public String toString() {
            return round(min) + "/" + round(round(sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MappedByteBufferReader {
        private final long maxReads;
        private long reads = 0L;
        private final MappedByteBuffer buf;

        public MappedByteBufferReader(MappedByteBuffer buf, long maxReads) {
            this.buf = buf;
            this.maxReads = maxReads;
        }

        public int getPosition() {
            return this.buf.position();
        }

        public void setReadPosition(int newPosition, int counterDiff) {
            this.buf.position(newPosition);
            this.reads -= counterDiff;
        }

        public int read() {
            if (reads + 1 > maxReads)
                return -1;

            var res = this.buf.get();
            reads++;
            return res;
        }
    }

    private static class EOFException extends Exception {
    }

    private static class Processor extends Thread {
        private final MappedByteBufferReader mbbReader;
        private HashMap<Long, Aggregate> map = new HashMap<>();
        private long offset;
        private long maxOffset;
        private final HashMap<Long, String> nameMapping;

        public HashMap<Long, Aggregate> getAggrMap() {
            return this.map;
        }

        // public HashMap<Long, String> getNameMapping() {
        // return this.nameMapping;
        // }

        Processor(RandomAccessFile file, long offset, long maxOffset, HashMap<Long, String> nameMapping) throws IOException {
            file.seek(offset);
            this.offset = offset;
            this.maxOffset = maxOffset;
            long maxCounter = maxOffset - offset + (maxOffset == file.length() ? 0 : 107);
            var buf = file.getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, offset, maxCounter);
            this.mbbReader = new MappedByteBufferReader(buf, maxCounter);
            this.nameMapping = nameMapping;
        }

        @Override
        public void run() {
            try {
                if (offset != 0) {
                    skipToStartOfKey(mbbReader);
                }

                while (true) {
                    if (offset + mbbReader.reads > maxOffset)
                        break;

                    int prev_position = mbbReader.getPosition();
                    var key = readKey(mbbReader);

                    var aggr = map.get(key);
                    if (aggr == null) {
                        aggr = Aggregate.getDefaultAggr();

                        if (!nameMapping.containsKey(key)) {
                            synchronized (this.nameMapping) {
                                if (!nameMapping.containsKey(key)) {
                                    mbbReader.setReadPosition(prev_position, mbbReader.getPosition() - prev_position);
                                    var keyStr = readKeyStr(mbbReader);
                                    nameMapping.put(key, keyStr);
                                }
                            }
                        }
                    }

                    var val = readVal(mbbReader);

                    map.put(key, aggr.withUpdated(val));
                }
            }
            catch (CalculateAverage_arun_murugan.EOFException ignored) {
            }
            catch (IOException ex) {
                System.err.println(ex);
            }
        }

        private void skipToStartOfKey(MappedByteBufferReader reader) throws IOException {
            while (true) {
                var val = reader.read();
                if (val == '\n' || val == -1)
                    break;
            }
        }

        private long readKey(MappedByteBufferReader reader) throws IOException, CalculateAverage_arun_murugan.EOFException {
            long hash = 0;

            while (true) {
                var val = reader.read();
                if (val == -1)
                    throw new CalculateAverage_arun_murugan.EOFException();
                if (val == ';') {
                    return hash;
                }

                hash = (char) val + (hash << 6) + (hash << 16) - hash;
            }
        }

        private String readKeyStr(MappedByteBufferReader reader) throws IOException, CalculateAverage_arun_murugan.EOFException {
            ArrayList<Byte> arr = new ArrayList<>();
            while (true) {
                var val = reader.read();
                if (val == -1)
                    throw new CalculateAverage_arun_murugan.EOFException();
                if (val == ';') {
                    var bytes = new byte[arr.size()];
                    for (int i = 0; i < arr.size(); i++) {
                        bytes[i] = arr.get(i);
                    }

                    return new String(bytes, StandardCharsets.UTF_8);
                }

                arr.add((byte) val);
            }
        }

        private float readVal(MappedByteBufferReader reader) throws IOException, CalculateAverage_arun_murugan.EOFException {
            float res = 0;
            int sign = 1;
            var val = reader.read();
            if (val == -1)
                throw new CalculateAverage_arun_murugan.EOFException();

            if (val == '-') {
                sign = -1;
            }
            else {
                res = val - '0';
            }

            while (true) {
                val = reader.read();
                if (val == -1)
                    throw new CalculateAverage_arun_murugan.EOFException();

                if (val == '.') {
                    val = reader.read();
                    reader.read();
                    if (val == -1)
                        throw new CalculateAverage_arun_murugan.EOFException();

                    res = res + (float) (val - '0') / 10;

                    return sign * res;
                }
                res = 10 * res + (val - '0');
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        long fileSize = file.length();
        var processors = new ArrayList<Processor>();
        int cores = Runtime.getRuntime().availableProcessors();
        int nThreads = Math.min((int) Math.ceil((double) fileSize / (100000 * 107)), cores);

        long partitionSize = (long) Math.ceil((double) fileSize / nThreads);
        long offset = 0, maxOffset = partitionSize;
        HashMap<Long, String> nameMapping = new HashMap<>();

        for (int i = 0; i < nThreads; i++) {
            Processor processor = new Processor(file, offset, maxOffset, nameMapping);
            processors.add(processor);
            processor.start();
            offset = maxOffset;
            maxOffset = Math.min(offset + partitionSize, fileSize);
        }

        TreeMap<String, Aggregate> result = new TreeMap<>();
        for (int i = nThreads - 1; i >= 0; i--) {
            Processor processor = processors.get(nThreads - i - 1);
            processor.join();

            processor.getAggrMap()
                    .forEach((key, value) -> result.merge(nameMapping.get(key), value, Aggregate::merge));
        }

        System.out.println(result);
    }
}
