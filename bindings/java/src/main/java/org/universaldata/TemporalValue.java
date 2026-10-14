package org.universaldata;
/** Proleptic Gregorian ticks since 0001-01-01; offset is absent for a local SQL value. */
public record TemporalValue(long ticks100Nanoseconds,Integer offsetMinutes) {}
