 package IPParser;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cloudera.io.netty.util.internal.StringUtil;

import IPParser.IPParser.RegionInfo;

public class LogParserProvince {

public Map<String,String> ParseLog (String log)
	{
	
	Map<String,String> logMap = new HashMap<>();
	
	  if (StringUtils.isNotBlank(log))
	  {
		  
		  String country = "-";
		  String province = "-";
		  String city = "-";
		  String url = "-";
		  String pageID = "-";
		  String time = "-";
		  String ip = "-";
	  
 
 
	  String[] values = log.split("\t");
	  
	  
	  ip = values[0];
	  country = values[1];
	  province = values[2];
	  city = values[3];
	  url = values[4];
	  time = values[5];
	  pageID = values[6];
	  
  
	  logMap.put("url", url);
	  logMap.put("time", time);
	  logMap.put("pageID", pageID);
	  logMap.put("ip", ip);
	  logMap.put("country", country);
	  logMap.put("province", province);
	  logMap.put("city", city);
	  
	  }
	  return logMap;
	  
	  
		
	}

}



