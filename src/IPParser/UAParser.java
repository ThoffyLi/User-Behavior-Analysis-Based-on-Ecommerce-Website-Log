package IPParser;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import java.io.IOException;


public class UAParser {

    static UASparser uasParser = null;

    // 初始化uasParser对象
    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
    	String str1 = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
        String str2 = "Mozilla/5.0	(Windows NT 6.1; WOW64)	AppleWebKit/537.36	(KHTML, like Gecko)	Chrome/44.0.2403.130	Safari/537.36";
        System.out.println(str1);
        try {
            UserAgentInfo userAgentInfo = UAParser.uasParser.parse(str1);
            System.out.println("操作系统名称："+userAgentInfo.getOsFamily());//
            //System.out.println("操作系统："+userAgentInfo.getOsName());//
            System.out.println("浏览器名称："+userAgentInfo.getUaFamily());//
            //System.out.println("浏览器版本："+userAgentInfo.getBrowserVersionInfo()); 
            System.out.println("设备类型："+userAgentInfo.getDeviceType());
            //System.out.println("浏览器:"+userAgentInfo.getUaName());
            //System.out.println("类型："+userAgentInfo.getType());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
	
	
	
}





 

 




