import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tools.ant.taskdefs.Classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class HiveCreateTable extends URLClassLoader{
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static String JARS_DIR = "file:///" + System.getProperty("user.dir") + "/src/main/resources";

  private HiveCreateTable(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException,
          IllegalAccessException, InstantiationException {

    System.out.println(JARS_DIR);
    System.out.println(new Path(JARS_DIR).toUri().toURL());
    System.out.println(getFilesInPath(new Path(JARS_DIR).toUri().toURL()));


    ClassLoader classloader = loadJar();
    Thread.currentThread().setContextClassLoader(classloader);
    Class.forName(driverName);

     //get connection
    String url = args[0];
    String port = args[1];
    String db = args[2];
    String hiveServerPrincipal = args[3];

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "Kerberos");
    UserGroupInformation.setConfiguration(conf);

    // login the user if a keytab file is present
    // otherwise, assume kinit was already done
    if(args.length >= 5) {
      String loginUser = args[4];
      String keytabFile = args[5];
      UserGroupInformation.loginUserFromKeytab(loginUser, keytabFile);
    }


    Connection con = DriverManager.getConnection("jdbc:hive2://"+url+":"+port+"/"+db+";principal="+hiveServerPrincipal+"", "", "");

    // create statement
    Statement stmt = con.createStatement();

    // execute statement
    ResultSet createTable = stmt.executeQuery("CREATE TABLE IF NOT EXISTS "
        +" employee ( eid int, name String, "
        +" salary String, destignation String)"
        +" COMMENT ‘Employee details’"
        +" ROW FORMAT DELIMITED"
        +" FIELDS TERMINATED BY ‘\t’"
        +" LINES TERMINATED BY ‘\n’"
        +" STORED AS TEXTFILE;");
    while (createTable.next()) {
      System.out.println(createTable.getString(1));
    }

    ResultSet showTables = stmt.executeQuery("show tables");
    while (showTables.next()) {
      System.out.println(showTables.getString(1));
    }

    System.out.println("Table employee created.");

     //show tables
    String sql = "show tables '" + "employee" + "'";
    System.out.println("Running: " + sql);
    con.close();
  }



  private static ClassLoader loadJar() throws MalformedURLException {
    final ClassLoader classLoader = HiveCreateTable.class.getClassLoader();
    List<URL> urls = new ArrayList<>();

    urls.addAll(getFilesInPath(new Path(JARS_DIR).toUri().toURL()));

    URL []urlArray = new URL[urls.size()];
    urls.toArray(urlArray);
      return new HiveCreateTable(urlArray, classLoader);

  }

  static List<URL> getFilesInPath(URL fileURL) throws MalformedURLException {
    List<URL> urls = new ArrayList<>();

    File file = new File(fileURL.getPath());
    if (file.isDirectory()) {
      File[] files = file.listFiles();

      if (files != null) {
        for (File innerFile : files) {
          if (innerFile.isFile()) {
            urls.add(innerFile.toURI().toURL());
          }
        }
      }

      if (!fileURL.toString().endsWith("/")) {
        fileURL = new URL(fileURL.toString() + "/");
      }
    }

    urls.add(fileURL);
    return urls;
  }
}