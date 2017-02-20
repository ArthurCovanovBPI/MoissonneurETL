package rfharvester.indexor;

/**
 * @author ArthurCovanov
 *
 */
public interface IndexorInterface
{
	public void indexUploads() throws IndexorException;
	public void end() throws IndexorException;
}