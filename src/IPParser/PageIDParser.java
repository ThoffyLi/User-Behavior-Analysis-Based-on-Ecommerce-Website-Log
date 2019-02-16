//Paring ip out of log


package IPParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class PageIDParser {

	
	public String getPageID(String url)
	{   if (StringUtils.isNotBlank(url)==false)
	    {return " ";}
	else
		{Pattern pattern  = Pattern.compile("topicId=[0-9]+");
		Matcher matcher = pattern.matcher(url);
		
		if (matcher.find())
		{
			String pageID = matcher.group().split("topicId=")[1];
		
		   return pageID;
		   }
		else
		{   

			return " ";
		}
		}
		
	}
	
	
	
	
}
