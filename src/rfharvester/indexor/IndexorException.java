package rfharvester.indexor;

/**
 * @author Arthur Covanov
 */
public class IndexorException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4265497957027740267L;

	public IndexorException()
	{
		super();
	}

	public IndexorException(String s)
	{
		super(s);
	}

	public IndexorException(Exception e)
	{
		super(e);
	}
}
