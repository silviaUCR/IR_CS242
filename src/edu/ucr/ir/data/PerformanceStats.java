package edu.ucr.ir.data;

import java.time.Duration;
import java.time.Instant;

// Misc performance stats
public class PerformanceStats {
    Instant startedAt = null;
    Instant lap = null;
    long counts = 0;

    // Constructor
    public void PerformanceStats()
    {
        init();
    }

    void init()
    {
        startedAt = lap = Instant.now();
        counts = 0;
    }

    public void reset()
    {
        init();
    }

    public long totalElapsedMilli()
    {
        return Duration.between(startedAt, Instant.now()).toMillis();
    }

    public long getLapMilli()
    {
        long result =  Duration.between(lap, Instant.now()).toMillis();
        lap = Instant.now();
        return result;
    }

    public long peekLapMilli()
    {
        return  Duration.between(lap, Instant.now()).toMillis();
    }

    public long count()
    {
        return counts++;
    }

    public long getCount()
    {
        return counts;
    }

    public double getAvgTime()
    {
        return totalElapsedMilli() / counts;
    }

    public String getString()
    {
        // Returns a string with the summary
        return String.format("Processed %d records in %d ms", getCount(), totalElapsedMilli());
    }

    public String getCSV()
    {
        // Returns elapsed,count
        return String.format("%d,%d", totalElapsedMilli(), getCount());
    }
}
