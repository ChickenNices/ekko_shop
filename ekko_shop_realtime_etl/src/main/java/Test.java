import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Test {
    public static void main(String[] args) throws MissingDissectorsException, NoSuchMethodException, DissectionFailure, InvalidDissectorException {
        new Test().run();
    }

    String logformat = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\" \"%{Addr}i\"";
    String logline = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; BuI=SomeThing; Apache=127.0.0.1.1351111543699529\" \"beijingshi\"";

    private void run() throws InvalidDissectorException, MissingDissectorsException, NoSuchMethodException, DissectionFailure {
        //打印日志格式的参数列表
        printAllPossibles(logformat);
}


    private static final Logger LOG = LoggerFactory.getLogger(Test.class);

    //打印所有参数
    private void printAllPossibles(String logformat) throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {
        Parser<Object> dummyParser = new HttpdLoglineParser<>(Object.class, logformat);
        List<String> possiblePaths = dummyParser.getPossiblePaths();
        dummyParser.addParseTarget(String.class.getMethod("indexOf", String.class), possiblePaths);

        LOG.info("==================================");
        System.out.println("==================================");
        LOG.info("Possible output:");
        System.out.println("Possible output:");
        for (String path : possiblePaths) {
            System.out.println(path + "     " + dummyParser.getCasts(path));
            LOG.info("{}     {}", path, dummyParser.getCasts(path));
        }
        System.out.println("==================================");
        LOG.info("==================================");
    }
}