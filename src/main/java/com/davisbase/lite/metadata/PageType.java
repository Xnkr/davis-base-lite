package com.davisbase.lite.metadata;

public enum PageType {
    INTERIOR((byte) 5),
    LEAF((byte) 13);

    private byte value;

    PageType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }
}
