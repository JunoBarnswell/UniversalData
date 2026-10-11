package org.universaldata;
public final class UniversalDataException extends RuntimeException {
    private final int status;
    public UniversalDataException(int status, String message) { super(message); this.status = status; }
    public int status() { return status; }
}
