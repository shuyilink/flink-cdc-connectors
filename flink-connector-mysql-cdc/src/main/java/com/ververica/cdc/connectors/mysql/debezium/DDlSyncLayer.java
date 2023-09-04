package com.ververica.cdc.connectors.mysql.debezium;

import com.ververica.cdc.connectors.mysql.DDLParser;
import org.antlr.v4.runtime.atn.BlockEndState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.relational.ddl.DdlParser;

public class DDlSyncLayer {

    private static final Logger LOG = LoggerFactory.getLogger(DDlSyncLayer.class);

    private String sinkJDBCURL;
    private String user;
    private String password;

    static private DDlSyncLayer instance = null;
    Connection connection = null;

    private boolean isInited;

    private final Pattern addDropDDLPattern;

    private static final String addDropDDLRegex = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s";


    public DDlSyncLayer() {
        this.addDropDDLPattern = Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE);
    }

    static public DDlSyncLayer getInstance()
    {
        if (instance == null)
        {
            instance = new DDlSyncLayer();
        }
        return instance;
    }

    void initConnection()
    {
//        String url =  String.format("jdbc:mysql://%s:%d/%s",host,port,db);

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(sinkJDBCURL, user , password);
        } catch (Exception e) {
            LOG.info("DDlSyncLayer initConnection failed {} {} {} {}",sinkJDBCURL,user,password,e);
            throw  new RuntimeException("DDlSyncLayer initConnection failed",e);
        }
    }

    public void initParam(String sinkJDBCURL,String user, String password)
    {
//        if(sinkJDBCURL.isEmpty() || sinkJDBCURL.length() == 0){
//            return;
//        }

//        this.sinkJDBCURL = sinkJDBCURL;
//        this.user = user;
//        this.password = password;

//        this.sinkJDBCURL =  "jdbc:mysql://193.169.203.10:31632/test";
//        this.user = "root";
//        this.password = "";
//        initConnection();
//        this.isInited = true;
    }

    public void execute(String db,String ddl) {
        String table = DDLParser.parseTableName(ddl);
        String op =  DDLParser.parseOperation(ddl);
        String column =  DDLParser.parseColumn(ddl);
        String comment  =  DDLParser.parseComment(ddl);
        String def = DDLParser.parseDefaultValue(ddl);
        String type = "";
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if(matcher.find()){
            type = matcher.group(5);
        }else {
            LOG.info("------------ type not found {}",ddl);
        }
        String starrocksDDL = String.format(EXECUTE_DDL,table,op,column,type);
        LOG.info("DDlSyncLayer return {} {} {} {} {} {} {} {} {}",db,table,op,column,comment,def,type,ddl,starrocksDDL);

        if(!isInited)
        {
            LOG.info("DDlSyncLayer return {} {}",db,starrocksDDL);
            return;
        }
        LOG.info("DDlSyncLayer execute start {} {}",db,starrocksDDL);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(starrocksDDL);
        } catch (SQLException e) {
            LOG.info("DDlSyncLayer execute failed {} {} {} ",db,starrocksDDL,e);
            throw new RuntimeException(e);
        }
        LOG.info("DDlSyncLayer execute succeed {} {}",db,starrocksDDL);
    }
}
