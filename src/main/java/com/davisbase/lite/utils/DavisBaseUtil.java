package com.davisbase.lite.utils;

import java.util.Random;

public class DavisBaseUtil {

    public static void printSuccess(String messsage){
        System.out.println("++++++++++++++ " + messsage + " ++++++++++++++++++++");
    }

    public static void printFail(String messsage){
        System.out.println("-------------- " + messsage + " --------------------");
    }

    public static int getRandomInRange(int max, int min){
        Random random = new Random();
        return random.nextInt((max - min) + 1) + min;
    }

    public static String getRandomDateTime(){
        int year = getRandomInRange(2100, 1900);
        int month = getRandomInRange(12, 1);
        int day = getRandomInRange(25, 1);
        int hour = getRandomInRange(23, 1);
        int min = getRandomInRange(59, 1);
        int sec = getRandomInRange(59, 1);
        return String.format("%d-%02d-%02d_%02d:%02d:%02d", year, month, day, hour, min, sec);
    }

    public static String getRandomString(){
        StringBuilder stringBuilder = new StringBuilder();
        int length = getRandomInRange(10, 5);
        for (int i = 0; i < length; i++) {
            char c = (char) getRandomInRange(122, 97);
            stringBuilder.append(c);
        }
        return stringBuilder.toString();
    }


}
