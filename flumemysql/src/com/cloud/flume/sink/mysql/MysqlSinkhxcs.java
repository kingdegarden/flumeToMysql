package com.cloud.flume.sink.mysql;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MysqlSinkhxcs extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(MysqlSinkhxcs.class);
    private String hostname;
    private String port;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;
    //sink监控
    //private SinkCounter sinkCounter;
    private static final String SEPARATOR_TAG = " ";
    private static final String END_TAG = "================end";

    public MysqlSinkhxcs() {
        LOG.info("MysqlSink start...");
    }

    //args checks
    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString("databaseName");
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        tableName = context.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        batchSize = context.getInteger("batchSize", 100);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
    }

    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName; 
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象

        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //创建一个Statement对象
            preparedStatement = conn.prepareStatement("insert into " + tableName + 
                                               " (date,time,loglevel,msg,host,url) values (?,?,?,?,?,?)");

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        String msg = null;
        String first[]= null;
        String contentall = null;
        List<String[]> actions = Lists.newArrayList();
        
        try {
        	transaction.begin();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    //---------------------------------------------
                    if(content==""){
                    	continue;
                    }
                    first = content.split(SEPARATOR_TAG);
                    
                    while((msg = new String(channel.take().getBody()))!=END_TAG)
                    contentall = contentall+"\n"+msg;
                    first[3] = msg;
//                    for(int i1=0;i1<6;i1++){
//                    	contentall[i] = first[i];
//                    }
//                    contentall[6] = msg;
                    actions.add(first);
                } else {
                    status = Status.BACKOFF;
                    break;
                }
            }

            if (actions.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp[] : actions) {
                	for(int i=0;i<temp.length;i++){
                		preparedStatement.setString(i+1, temp[i]);
                	}
//                    preparedStatement.setString(1, temp[0]);
//                    preparedStatement.setString(2, temp[1]);
//                    preparedStatement.setString(3, temp[2]);
//                    preparedStatement.setString(4, temp[3]);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                conn.commit();
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }

        return status;
    }
}