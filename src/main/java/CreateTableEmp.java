
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTableEmp extends HttpServlet {

    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
//        out.println("<h1>Hello Servlte</h1>");
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.toString(), "thrift://hadoop001:10000");
        HiveMetaStoreClient hmsc = null;
        try {
            hmsc = new HiveMetaStoreClient(conf);
            if (hmsc.tableExists("soctt", "emp_create_by_meta_api"))
                hmsc.dropTable("soctt", "emp_create_by_meta_api");

            List<FieldSchema> columns = new ArrayList<>();   // columns_v2
            columns.add(new FieldSchema("empno", "int", ""));
            columns.add(new FieldSchema("ename", "string", ""));
            columns.add(new FieldSchema("job", "string", ""));
            columns.add(new FieldSchema("mgr", "string", ""));
            columns.add(new FieldSchema("hiredate", "string", ""));
            columns.add(new FieldSchema("sal", "double", ""));
            columns.add(new FieldSchema("comm", "double", ""));
            columns.add(new FieldSchema("deptno", "int", ""));

            Map param = new HashMap<String, String>(1);
            param.put("field.delim", "\t");  //serde_params
            StorageDescriptor storageDescriptor
                    = new StorageDescriptor(columns, null,
                    "org.apache.hadoop.mapred.TextInputFormat", // sds
                    "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", // sds
                    false, 0,
                    // serdes
                    new SerDeInfo("LBCSerDe","org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", new HashMap<>()),
                    null, null, null);
            Table tbl = new Table("emp_create_by_meta_api", "soctt","root",
                    0, 0, 0, storageDescriptor, null, param, "", "",
                    "MANAGED_TABLE"); //tbls

            hmsc.createTable(tbl);
            out.write("<h2>Create success!!</h2>");
        } catch (Exception e) {
            out.write("Create failed!!");
            e.printStackTrace();
        }
        //System.out.println(hmsc.getAllDatabases());‍



    }
}
