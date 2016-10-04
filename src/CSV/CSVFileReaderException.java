package CSV;

public class CSVFileReaderException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7507360541086440998L;

	public CSVFileReaderException()
	{
		super();
	}
	
	public CSVFileReaderException(Exception e)
	{
		super(e);
	}
	
	public CSVFileReaderException(String message)
	{
		super(message);
	}
}
