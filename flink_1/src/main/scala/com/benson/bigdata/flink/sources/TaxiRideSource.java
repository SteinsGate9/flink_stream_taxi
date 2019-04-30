package com.benson.bigdata.flink.sources;
import com.benson.bigdata.flink.datatypes.TaxiRide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class TaxiRideSource implements SourceFunction<TaxiRide> {

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public TaxiRideSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;                                  // 60000
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs; // 60000 maxDelaysec
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<TaxiRide> sourceContext) throws Exception {
        ZipFile zf = new ZipFile(dataFilePath);
        ZipInputStream zin = new ZipInputStream(new BufferedInputStream(new FileInputStream(dataFilePath)),Charset.forName("gbk"));
        ZipEntry ze = zin.getNextEntry();
        reader = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));

        generateUnorderedStream(sourceContext);

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
    }

    private void generateUnorderedStream(SourceContext<TaxiRide> sourceContext) throws Exception {
        // get times
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        // randim
        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        // read first ride and insert it into emit schedule
        String line;
        TaxiRide ride;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first ride
            ride = TaxiRide.fromString(line);

            // add
            // get delayed time
            // extract starting timestamp
            dataStartTime = ride.getEventTime();
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);
            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

            // add watermarkTime
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

        } else {
            return;
        }

        // peek at next ride
        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
        }

        // read rides one-by-one and emit a random ride from the buffer each time
        while (emitSchedule.size() > 0 || reader.ready()) {

            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = ride != null ? ride.getEventTime() : -1; // null->-1 vs peek()
            int i = 0;
            while(
                    ride != null && ( // while there is a ride AND
                            emitSchedule.isEmpty() || // and no ride in schedule OR
                                    rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // eventtime < first peek + 60s              not enough rides in schedule
                    )
            {
                // insert event into emit schedule
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

                // read next ride
                if (reader.ready() && (line = reader.readLine()) != null) {
                    ride = TaxiRide.fromString(line);
                    rideEventTime = ride.getEventTime();
                }
                else {
                    ride = null;
                    rideEventTime = -1;
                }
                i = i + 1 ;
//                System.out.println(ride.startTime);
            }
//            System.out.println("HGG");
            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0; // this head
            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime); // (this - first / 600)
            long waitTime = servingTime - now;
            Thread.sleep( (waitTime>0) ? waitTime:0);

            if(head.f1 instanceof TaxiRide) {
                TaxiRide emitRide = (TaxiRide)head.f1;
                // emit ride
                sourceContext.collectWithTimestamp(emitRide, emitRide.getEventTime());
//                System.out.println("HGG");
            }
            else if(head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark)head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);
                // schedule next watermark
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
            }
        }
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }

}

