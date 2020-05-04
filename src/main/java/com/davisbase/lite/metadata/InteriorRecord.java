package com.davisbase.lite.metadata;

public class InteriorRecord {

    public int rowId;
    public int leftChildPageNo;

    public InteriorRecord(int rowId, int leftChildPageNo) {
        this.rowId = rowId;
        this.leftChildPageNo = leftChildPageNo;
    }
}
