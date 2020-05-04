package com.davisbase.lite.metadata;

public enum DataType {

    NULL((byte)0){
        @Override
        public String toString(){ return "NULL"; }},
    TINYINT((byte)1){
        @Override
        public String toString(){ return "TINYINT"; }},
    SMALLINT((byte)2){
        @Override
        public String toString(){ return "SMALLINT"; }},
    INT((byte)3){
        @Override
        public String toString(){ return "INT"; }},
    BIGINT((byte)4){
        @Override
        public String toString(){ return "BIGINT"; }},
    FLOAT((byte)5){
        @Override
        public String toString(){ return "FLOAT"; }},
    DOUBLE((byte)6){
        @Override
        public String toString(){ return "DOUBLE"; }},
    YEAR((byte)8){
        @Override
        public String toString(){ return "YEAR"; }},
    TIME((byte)9){
        @Override
        public String toString(){ return "TIME"; }},
    DATETIME((byte)10){
        @Override
        public String toString(){ return "DATETIME"; }},
    DATE((byte)11){
        @Override
        public String toString(){ return "DATE"; }},
    TEXT((byte)12){
        @Override
        public String toString(){ return "TEXT"; }};

    private byte value;

    public byte getValue() {
        return value;
    }

    public void setValue(byte value) {
        this.value = value;
    }

    DataType(byte value) {
        this.value = value;
    }

}
