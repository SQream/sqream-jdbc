package com.sqream.jdbc;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;


public class DatetimeLogic {
    
    static Date dateFromInt(int dateNum, boolean dt_fix)
    {
        java.sql.Date dt = null;
        long year = ((long)10000*dateNum + 14780)/3652425;
        long xx = 365*year + year/4 - year/100 + year/400;
        long ddd =  (dateNum - xx);

        if (ddd < 0) 
        {
             year = year - 1;
            ddd = (dateNum - (365*year + year/4 - year/100 + year/400));
        }
 
        long mi = (long)(100*ddd + 52)/3060;
        long month = ((mi + 2)%12) + 1;
        year = year + (mi + 2)/12;
        if (dt_fix) {
            if (year<1900)
                year+=1900;
            if (month==12)
                month = 1;
            else
                month+=1;   
        }
        long day = ddd - (mi*306 + 5)/10 + 1;
        String dateStr = year+"-"+month+"-"+day;
        dt = java.sql.Date.valueOf(dateStr);
        //Console.WriteLine(dt.ToString());
        return dt;          
    }
    
    
    static long convertTimeStampToLong(Timestamp ts) {
		long result = 0;
		int nDate = 0;
		int nTime = 0;
		if (ts != null) {
			Calendar c = Calendar.getInstance();
			c.setTime(ts);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH) + 1;
			int day = c.get(Calendar.DAY_OF_MONTH);

			month = (month + 9) % 12;
			year = year - month / 10;

			nDate = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));
		}
		nTime = nTime + ts.getHours() * 3600000;
		nTime = nTime + ts.getMinutes() * 60000;
		nTime = nTime + ts.getSeconds() * 1000;
		nTime = nTime + (ts.getNanos() / 1000000);

		result = (((long) nDate) << 32) | (nTime & 0xffffffffL);
		return result;
	}
    
}