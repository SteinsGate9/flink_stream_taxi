package com.benson.bigdata.flink.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.Locale;
import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {
    public static void main(String[] args) throws IOException {
        TaxiRide i = new TaxiRide();
        i = i.fromString("24/08/2008 00:00:13, SH1033G, 103.98562, 1.35078, 1, BUSY");
        System.out.println(i.startLat);
        System.out.println(i.startTime);
    }

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public TaxiRide() {
        this.startTime = new DateTime();
    }

    public TaxiRide(DateTime startTime, String taxiId,
                    float startLon, float startLat,
                    long wtf, String wtf2) {
        this.startTime = startTime;
        this.taxiId = taxiId;
        this.startLon = startLon;
        this.startLat = startLat;
    }

    public DateTime startTime;
    public String taxiId;
    public float startLon;
    public float startLat;


    public static TaxiRide fromString(String line) {
        // time / id / lo / la / w1 /w2
        String[] tokens = line.split(", ");
        if (tokens.length != 6) {
            throw new RuntimeException("Invalid record: " + line);
        }
        TaxiRide ride = new TaxiRide();

        try{
        ride.startTime = DateTime.parse(tokens[0], timeFormatter).minusHours(8);
        ride.taxiId = tokens[1];
        ride.startLon = tokens[2].length() > 0 ? Float.parseFloat(tokens[2]) : 0.0f;
        ride.startLat = tokens[3].length() > 0 ? Float.parseFloat(tokens[3]) : 0.0f;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return ride;
    }

    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    public int compareTo(TaxiRide other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
            return compareTimes;
    }

    @Override
    public boolean equals(Object other) {
        System.out.println("equal");
        return other instanceof TaxiRide &&
                this.taxiId == ((TaxiRide) other).taxiId;
    }


    public long getEventTime() {
        return startTime.getMillis();
    }
}
