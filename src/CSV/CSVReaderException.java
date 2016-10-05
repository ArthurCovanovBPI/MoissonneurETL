package CSV;

public class CSVReaderException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7507360541086440998L;

	public CSVReaderException()
	{
		super();
	}
	
	public CSVReaderException(Exception e)
	{
		super(e);
	}
	
	public CSVReaderException(String message)
	{
		super(message);
	}
}
