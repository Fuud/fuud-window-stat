package fuud.windowstat.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

public class MockClock extends Clock {
    private final AtomicLong millis;

    private final ZoneId zoneId;

    public MockClock() {
        zoneId = ZoneId.systemDefault();
        millis = new AtomicLong(0);
    }

    private MockClock(ZoneId zoneId, AtomicLong millis) {
        this.zoneId = zoneId;
        this.millis = millis;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MockClock(zone, millis);
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }

    public void setTime(long timeMs) {
        millis.set(timeMs);
    }

    public void move(long ms) {
        millis.addAndGet(ms);
    }

}