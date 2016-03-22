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
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

public class MysqlSinkError extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(MysqlSinkError.class);
    private String hostname;
    private String port;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;
    //sink¼à¿Ø
    //private SinkCounter sinkCounter;
    private static final String SEPARATOR_TAG = "~";
    private static final String regex = "[a-zA-Z]*~[a-zA-Z]*";
    private static Pattern pattern = Pattern.compile(regex);
    private static boolean isError;

    public MysqlSinkError() {
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
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName; 

        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement("insert into " + tableName + 
                                               " (date,msg) values (?,?)");

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
        StringBuilder contentelse = new StringBuilder();
        String all[] = null;
        List<String[]> actions = Lists.newArrayList();
        
        try {
        	transaction.begin();
            for (int i = 0; i < batchSize; i++) {
            	while((event = channel.take())!=null){
            		content = new String(event.getBody());
            		if(pattern.matcher(content).find()){
            			if(all!=null){
            				if(all.length==2){
            					all[1] = all[1] + contentelse.toString();
            					contentelse.delete(0, contentelse.length());
            				}
            				actions.add(all);
            			}
            			all = content.split(SEPARATOR_TAG);
            		}
            		else{
            			if(content!=null){
            				contentelse.append("\n"+content);
            			}
            		}
            	}
            	
                if (event == null) {
                	status = Status.BACKOFF;
                    break;
                } 
            }

            if (actions.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp[] : actions) {
                    preparedStatement.setString(1, temp[0]);
                    preparedStatement.setString(2, temp[1]);
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