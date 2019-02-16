//  Parsing information from log

package IPParser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;

import com.cloudera.io.netty.util.internal.StringUtil;

import IPParser.IPParser.RegionInfo;
import cz.mallat.uasparser.UserAgentInfo;

public class LogParser {

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
		  String user_agent = "-";
	      String os = "-";
	      String device = "-";
	      String browser = "-";
	      String referer_url = "-";
	      String referer = "-";
	      
	 //parse pageid	  
	  PageIDParser pageidparser = new PageIDParser();
	  pageID = pageidparser.getPageID(log);
	 
	  
	  
	  
	  
	  String[] values = log.split("\001");
	
	  url = values[1];
	  time = values[17];
	  ip = values[13];
	  user_agent = values[29];
	  referer_url = values[2];
	  
	  //parsing referer
	  Pattern pattern = Pattern.compile("[a-z]+[.][a-z]+[.][a-z]+");
	  Matcher matcher = pattern.matcher(referer_url);
	  
	  if(matcher.find())
	  {
		  referer = matcher.group().split("\\.")[1];
	  }
	  
	  
	  
	  //parsing UA
	  try {
          UserAgentInfo userAgentInfo = UAParser.uasParser.parse(user_agent);
          os = userAgentInfo.getOsFamily(); //operating system
          browser = userAgentInfo.getUaFamily();
          device = userAgentInfo.getDeviceType();

      } catch (IOException e) {
          e.printStackTrace();
      }
	  
 
	//parsing IP
	  IPParser ipparser = IPParser.getInstance();
	  RegionInfo info = ipparser.analyseIp(ip);
	  
	  if (info != null)
	  {
	   country = info.getCountry();
	   province = info.getProvince();
	   city = info.getCity();
	  }
	  
	  logMap.put("referer", referer);
	  logMap.put("url", url);
	  logMap.put("os", os);
	  logMap.put("device", device);
	  logMap.put("browser", browser);
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



