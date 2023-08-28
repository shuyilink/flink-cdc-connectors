package com.ververica.cdc.connectors.mysql.debezium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DDlSyncLayer {

    private static final Logger LOG = LoggerFactory.getLogger(DDlSyncLayer.class);

    private String host;
    private String user;
    private String password;

    private int port;
    private String db;

    static private DDlSyncLayer instance = null;
    Connection connection = null;

    private boolean isInited;
    public DDlSyncLayer() {}

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
        String url =  String.format("jdbc:mysql://%s:%d/%s",host,port,db);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, user , password);
        } catch (Exception e) {
            LOG.info("DDlSyncLayer initConnection failed {} {} {} {} {} {}",url, host,port,user,password, db,e);
            throw  new RuntimeException("DDlSyncLayer initConnection failed",e);
        }
    }

    public void initParam(String host, int port, String user, String password, String db)
    {
        this.isInited = true;
        this.host = host;
        this.port = port;

        this.user = user;
        this.password = password;
        this.db = db;
        initConnection();
    }

    public void execute(String db,String sql) {
        if(!isInited)
        {
            LOG.info("DDlSyncLayer return {} {}",db,sql);
            return;
        }
        LOG.info("DDlSyncLayer execute start {} {}",db,sql);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(String.format("use %s",db));
            statement.execute(sql);
        } catch (SQLException e) {
            LOG.info("DDlSyncLayer execute failed {} {} {} ",db,sql,e);
            throw new RuntimeException(e);
        }
        LOG.info("DDlSyncLayer execute succeed {} {}",db,sql);
    }
}
