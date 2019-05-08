package ru.mail.polis.brainlux;

final class TimeUtils {
    private static long lastTime;
    private static int counter;

    private TimeUtils() {
    }

    static long getTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != lastTime) {
            lastTime = currentTime;
            counter = 0;
        }
        return currentTime * 1_000_000 + counter++;
    }

}