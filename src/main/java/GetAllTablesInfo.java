
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class GetAllTablesInfo extends HttpServlet {

    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
//        out.println("<h1>Hello Servlte</h1>");
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.toString(), "thrift://hadoop001:10000");
        HiveMetaStoreClient hmsc = null;
        try {
            hmsc = new HiveMetaStoreClient(conf);
            List<String> tables = hmsc.getAllTables("soctt");
            for(String t : tables) {
                out.write(String.format("table : %s<br/>",t));
//                List<FieldSchema> list = hmsc.getFields("soctt", t);
//                for(FieldSchema f : list) {
//                    //                System.out.println("\t column name is :" + f.getName());
//                    out.write(String.format("    %s<br/>", f.toString()));
//                }
//                out.write("<br/>");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println(hmsc.getAllDatabases());‍



    }
}
