package redis.command;

public enum Mode {
    GET_ACK("GETACK"),
    ACK("ACK"),
    LISTENING_PORT("LISTENING-PORT"),
    CAPA("CAPA");

    private final String mode;

    Mode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
