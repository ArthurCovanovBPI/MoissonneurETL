package rfharvester.logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class used to get date and hour in correct and standardized format<br/>
 * Never instantiated.
 * 
 * @author ArthurCovanov
 */
public abstract class RFHarvesterDatation
{
	private static SimpleDateFormat dateHourFormat = new SimpleDateFormat("dd/MM/yyyy - HH:mm:ss.SSS");
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
	private static SimpleDateFormat hourFormat = new SimpleDateFormat("HH:mm:ss.SSS");

	/**
	 * @return Transform year,month,day,hour,minute,second,millisecond to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month, int day, int hour, int minute, int second, int millisecond) throws ParseException
	{
		return dateHourFormat.parse(String.format("%02d/%02d/%04d - %02d:%02d:%02d.%03d", day, month, year, hour, minute, second, millisecond));
	}

	/**
	 * @return Transform year,month,day,hour,minute,second to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month, int day, int hour, int minute, int second) throws ParseException
	{
		return DataToDate(year, month, day, hour, minute, second, 0);
	}

	/**
	 * @return Transform year,month,day,hour,minute to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month, int day, int hour, int minute) throws ParseException
	{
		return DataToDate(year, month, day, hour, minute, 0, 0);
	}

	/**
	 * @return Transform year,month,day,hour to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month, int day, int hour) throws ParseException
	{
		return DataToDate(year, month, day, hour, 0, 0, 0);
	}

	/**
	 * @return Transform year,month,day to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month, int day) throws ParseException
	{
		return DataToDate(year, month, day, 0, 0, 0, 0);
	}

	/**
	 * @return Transform year,month to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year, int month) throws ParseException
	{
		return DataToDate(year, month, 0, 0, 0, 0, 0);
	}

	/**
	 * @return Transform year to Date
	 * @throws ParseException
	 */
	public static Date DataToDate(int year) throws ParseException
	{
		return DataToDate(year, 0, 0, 0, 0, 0, 0);
	}

	/**
	 * @return "dd/MM/yyyy - HH:mm:ss.SSS"
	 */
	public static String getDateHour()
	{
		return getDateHour(System.currentTimeMillis());
	}

	/**
	 * @param millis Milliseconds to format
	 * @return "dd/MM/yyyy - HH:mm:ss.SSS"
	 */
	public static String getDateHour(long millis)
	{
		return dateHourFormat.format(millis);
	}

	/**
	 * @return "dd/MM/yyyy"
	 */
	public static String getDate()
	{
		return getDate(System.currentTimeMillis());
	}

	/**
	 * @param millis Milliseconds to format
	 * @return "dd/MM/yyyy"
	 */
	public static String getDate(long millis)
	{
		return dateFormat.format(millis);
	}

	/**
	 * @return "HH:mm:ss.SSS"
	 */
	public static String getHour()
	{
		return getHour(System.currentTimeMillis());
	}

	/**
	 * @param millis Milliseconds to format
	 * @return "HH:mm:ss.SSS"
	 */
	public static String getHour(long millis)
	{
		return hourFormat.format(millis);
	}

	/**
	 * @param millis Milliseconds to convert
	 * @return Formated day(s) hour(s) minute(s) second(s) millisecond(s)
	 */
	public static String duration(long millis)
	{
		long f = 86400000;	// 1 day = 86400000 milliseconds
		long d = millis / f;
		millis = millis % f;
		f = 3600000;		// 1 hour = 3600000 milliseconds
		long h = millis / f;
		millis = millis % f;
		f = 60000;			// 1 minute = 60000 milliseconds
		long m = millis / f;
		millis = millis % f;
		f = 1000;			// 1 second = 1000 milliseconds
		long s = millis / f;
		millis = millis % f;

		return String.format("%2d day%c %2d hour%c %2d minute%c %2d second%c %03d millisecond%s", d, (d > 1) ? 's' : ' ', h, (h > 1) ? 's' : ' ', m, (m > 1) ? 's' : ' ', s, (s > 1) ? 's' : ' ', millis, (millis > 1) ? "s" : "");
	}
}