package com.ververica.cdc.connectors.mysql.debezium;

import com.ververica.cdc.connectors.mysql.DDLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDlSyncExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DDlSyncExecutor.class);

    private String ddlCaptureJDBCURL;
    private String user;
    private String password;

    static private DDlSyncExecutor instance = null;
    Connection connection = null;

    private boolean isInited;

    private final Pattern addDropDDLPattern;

    private static final String addDropDDLRegex = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s";

    public DDlSyncExecutor() {
        this.addDropDDLPattern = Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE);
    }

    static public DDlSyncExecutor getInstance()
    {
        if (instance == null)
        {
            instance = new DDlSyncExecutor();
        }
        return instance;
    }

    void initConnection()
    {
        try {
            connection = DriverManager.getConnection(ddlCaptureJDBCURL, user , password);
        } catch (Exception e) {
            LOG.info("DDlSyncExecutor initConnection failed {} {} {} {}",ddlCaptureJDBCURL,user,password,e);
            throw  new RuntimeException("DDlSyncLayer initConnection failed",e);
        }
    }

    public void init(String ddlCaptureJDBCURL,String user, String password)
    {
        LOG.info("DDlSyncExecutor init {} {} {}",ddlCaptureJDBCURL,user,password);
        if(ddlCaptureJDBCURL.isEmpty() || ddlCaptureJDBCURL.length() == 0){
            return;
        }

        this.ddlCaptureJDBCURL = ddlCaptureJDBCURL;
        this.user = user;
        this.password = password;
    }

    public void execute(String db,String ddl) throws SQLException {
        String table = DDLParser.parseTableName(ddl);
        String op =  DDLParser.parseOperation(ddl);
        String column =  DDLParser.parseColumn(ddl);
        String comment  =  DDLParser.parseComment(ddl);
        String def = DDLParser.parseDefaultValue(ddl);
        String type = "";
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if(matcher.find()){
            type = matcher.group(5);
        }

        String finalDDL = "";
        if (op.equalsIgnoreCase("drop")){
            finalDDL = String.format(EXECUTE_DDL,table,op,column,"");
        }else{
            finalDDL = String.format(EXECUTE_DDL,table,op,column,type);
        }
        LOG.info("DDlSyncExecutor execute {} {} {} {} {} {} {} {} {}",db,table,op,column,comment,def,type,ddl,finalDDL);

        initConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(finalDDL);
        } catch (SQLException e) {
            LOG.info("DDlSyncExecutor execute failed {} {} {} ",db,finalDDL,e);
            connection.close();
            return;
//            throw new RuntimeException(e);
        }
        connection.close();
        LOG.info("DDlSyncExecutor execute succeed {} {}",db,finalDDL);
    }
}
