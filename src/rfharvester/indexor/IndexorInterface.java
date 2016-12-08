package rfharvester.indexor;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author ArthurCovanov
 *
 */
public interface IndexorInterface
{
	public void indexUploads() throws IndexorException;
	public void end() throws IndexorException;
}