package com.sqream.jdbc.utils;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Utils {

    public static LocalDate intToLocalDate(int date_as_int) {

        long yy = ((long)10000*date_as_int + 14780)/3652425;
        long ddd =  (date_as_int - (365*yy + yy/4 - yy/100 + yy/400));

        if (ddd < 0) {
            yy -=  1;
            ddd = (date_as_int - (365*yy + yy/4 - yy/100 + yy/400));
        }

        long mi = (100*ddd + 52)/3060;

        int year = (int) (yy + (mi + 2)/12);
        int month = (int)((mi + 2)%12) + 1;
        int day = (int) (ddd - (mi*306 + 5)/10 + 1);

        return LocalDate.of(year, month, day);
    }

    public static Date intToDate(int date_as_int, ZoneId zone) {
        LocalDateTime local_dt = intToLocalDate(date_as_int).atStartOfDay();
        return new Date(Timestamp.from(local_dt.atZone(zone).toInstant()).getTime());
    }

    public static int dateToInt(Date d ,ZoneId zone) {

        // Consider a different implementation here
        if (d == null) {
            return 0;
        }

        LocalDate date = d.toLocalDate();
        int year = date.getYear();
        int month = date.getMonthValue();
        int day   = date.getDayOfMonth();

        month = (month + 9) % 12;
        year = year - month / 10;

        return (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));
    }

    public static long dtToLong(Timestamp ts, ZoneId zone) {  // ZonedDateTime

        if (ts == null)
            return 0;

        LocalDateTime datetime = ts.toInstant().atZone(zone).toLocalDateTime();

        //LocalDateTime datetime = ts.toLocalDateTime();
        int year  = datetime.getYear();
        int month = datetime.getMonthValue();
        int day   = datetime.getDayOfMonth();

        month = (month + 9) % 12;
        year = year - month / 10;

        int date_as_int = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));

        int time_as_int =  datetime.getHour() * 3600000;
        time_as_int += datetime.getMinute() * 60000;
        time_as_int += datetime.getSecond() * 1000;
        time_as_int += datetime.getNano() / 1000000;


        return (((long) date_as_int) << 32) | (time_as_int & 0xffffffffL);
    }

    public static Timestamp longToDt(long dt_as_long, ZoneId zone) {

        int date_as_int = (int)(dt_as_long >> 32);
        int time_as_int = (int)dt_as_long;

        // Get hour, minutes and seconds from the time part
        int hour = time_as_int / 1000 / 60 / 60;
        int minutes = (time_as_int / 1000 / 60) % 60 ;
        int seconds = ((time_as_int) / 1000) % 60;
        int ms = time_as_int % 1000;
        LocalDateTime local_dt = LocalDateTime.of(Utils.intToLocalDate(date_as_int), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6)));

        // return Timestamp.valueOf(local_dt);
        return Timestamp.from(local_dt.atZone(zone).toInstant());
    }

    public static Date dateFromTuple(int year, int month, int day) {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    public static Timestamp dateTimeFromTuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {

        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6))));
    }

    public static String formJson(String command) {
        String SIMPLE_MESSAGE = "'{'\"{0}\":\"{0}\"'}'";
        return MessageFormat.format(SIMPLE_MESSAGE, command);
    }

    public static String decode(ByteBuffer message) {
        return UTF_8.decode(message).toString();
    }
}
