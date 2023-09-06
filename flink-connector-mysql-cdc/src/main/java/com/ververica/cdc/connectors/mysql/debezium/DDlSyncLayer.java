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

    private String ddlCaptureJDBCURL;
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
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(ddlCaptureJDBCURL, user , password);
        } catch (Exception e) {
            LOG.info("DDlSyncLayer initConnection failed {} {} {} {}",ddlCaptureJDBCURL,user,password,e);
            throw  new RuntimeException("DDlSyncLayer initConnection failed",e);
        }
    }

    public void init(String ddlCaptureJDBCURL,String user, String password)
    {
        LOG.info("DDlSyncLayer init {} {} {}",ddlCaptureJDBCURL,user,password);
        if(ddlCaptureJDBCURL.isEmpty() || ddlCaptureJDBCURL.length() == 0){
            return;
        }

        this.ddlCaptureJDBCURL = ddlCaptureJDBCURL;
        this.user = user;
        this.password = password;

        initConnection();
        this.isInited = true;
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
        }

        String finalDDL = "";
        if (op.equalsIgnoreCase("drop")){
            finalDDL = String.format(EXECUTE_DDL,table,op,column,"");
        }else{
            finalDDL = String.format(EXECUTE_DDL,table,op,column,type);
        }
        LOG.info("DDlSyncLayer execute {} {} {} {} {} {} {} {} {}",db,table,op,column,comment,def,type,ddl,finalDDL);
        if(!isInited)
        {
            initConnection();
            if (!isInited){
                LOG.info("DDlSyncLayer init failed twice {} {}",db,finalDDL);
                return;
            }
        }

        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(finalDDL);
        } catch (SQLException e) {
            LOG.info("DDlSyncLayer execute failed {} {} {} ",db,finalDDL,e);
            return;
//            throw new RuntimeException(e);
        }
        LOG.info("DDlSyncLayer execute succeed {} {}",db,finalDDL);
    }
}
