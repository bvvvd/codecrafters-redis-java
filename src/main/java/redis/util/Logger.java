package redis.util;

public abstract class Logger {

    public static void debug(String message, Object... args) {
        log("DEBUG", message, args);
    }

    public static void warn(String message, Object... args) {
        log("WARN", message, args);
    }

    public static void error(String message, Object... args) {
        log("ERROR", message, args);
    }

    private static void log(String level, String message, Object... args) {
        if (args.length > 0) {
            message = String.format(message, args);
        }
        System.err.println(level + ": " + message);
    }
}
