package com.alibaba.datax.plugin.writer.mysqlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName MysqlAutoCreateTable
 * @Description mysql自动建表实现
 * @Author sunhaonan
 * @Date 2024/2/1 10:36
 * @Version 1.0
 */
class MysqlAutoCreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlAutoCreateTable.class);

    private static final String AUTO_CREATE_TABLE = "autoCreateTable";

    private static final Pattern CREATE_TABLE_PATTERN = Pattern
            .compile("\\s*[c,C][r,R][e,E][a,A][t,T][e,E]\\s+[t,T][a,A][b,B][l,L][e,E]\\s+(`?\\w+`?)\\s+");

    private static final Pattern SELECT_PATTERN = Pattern
            .compile("[f,F][r,R][o,O][m,M]\\s+(`?\\w+`?)");

    static void createTable(Configuration readerConfiguration, Configuration writerConfiguration) {
        // 首先判断标志位，如果为false直接返回了
        if (!writerConfiguration.getBool(AUTO_CREATE_TABLE, false))
            return;

        List<Object> connections = writerConfiguration.getList(Constant.CONN_MARK, Object.class);
        // 确定目的端表的数目
        int tableNum = 0;

        for (Object connection : connections) {
            Configuration connConf = Configuration.from(connection.toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DataBaseType.MySql, tables);

            if (expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();
        }

        if (tableNum > 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "您配置了自动建表，但是Writer端表的数量大于1，目前只支持Writer端表的数量为1");
        }

        // 确定Reader端的表名,区分 采用tableMode 还是SqlModel
        ConnectionInfo readerConnectionInfo = getReaderConnectionInfo(readerConfiguration);

        // 目前Reader端只支持mysql
        if (!readerConnectionInfo.getJdbcUrl().toLowerCase().contains("mysql")) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "您配置了自动建表，目前自动建表只支持mysql到mysql...");
        }

        Connection readerConnection = DBUtil.getConnection(DataBaseType.MySql, readerConnectionInfo.getJdbcUrl(),
                readerConnectionInfo.getUsername(), readerConnectionInfo.getPassword());

        String readerCreateSql = getReaderCreateTableSql(readerConnectionInfo, readerConnection);

        ConnectionInfo writerConnectionInfo = getWriterConnectionInfo(writerConfiguration);

        createWriterTable(writerConnectionInfo, readerCreateSql);
    }

    private static ConnectionInfo getReaderConnectionInfo(Configuration readerConfiguration) {
        String firstTableName = readerConfiguration.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE), null);

        String firstQuerySql = readerConfiguration.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, "querySql"), null);

        String tableName = null;
        if (firstTableName != null) {
            tableName = firstTableName;
        } else {
            Matcher matcher1 = SELECT_PATTERN.matcher(firstQuerySql);
            if (matcher1.find()) {
                tableName = matcher1.group(1);
            }
        }

        ConnectionInfo result = new ConnectionInfo();
        result.setJdbcUrl(readerConfiguration.getString(String.format("%s[0].%s[0]",
                Constant.CONN_MARK, Key.JDBC_URL)));
        result.setUsername(readerConfiguration.getString(Key.USERNAME));
        result.setPassword(readerConfiguration.getString(Key.PASSWORD));
        result.setTablename(tableName);

        return result;
    }

    private static ConnectionInfo getWriterConnectionInfo(Configuration writerConfiguration) {
        ConnectionInfo result = new ConnectionInfo();
        result.setJdbcUrl(writerConfiguration.getString(String.format("%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL)));
        result.setUsername(writerConfiguration.getString(Key.USERNAME));
        result.setPassword(writerConfiguration.getString(Key.PASSWORD));
        result.setTablename(writerConfiguration.getString(String.format("%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)));
        return result;
    }


    private static String getReaderCreateTableSql(ConnectionInfo connectionInfo, Connection connection) {
        String showCreateTableSql = "SHOW CREATE TABLE " + connectionInfo.getTablename();
        ResultSet rs = null;
        String readerCreateSql = null;
        try {
            rs = DBUtil.query(connection, showCreateTableSql);
            if (rs.next()) {
                readerCreateSql = rs.getString(2);
            }
        } catch (SQLException e) {
            throw RdbmsException.asQueryException(DataBaseType.MySql, e, showCreateTableSql, connectionInfo.getTablename(), null);
        } finally {
            DBUtil.closeDBResources(rs, null, connection);
        }
        if (readerCreateSql == null) {
            throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR,
                    "查询不到Reader端建表语句.");
        }
        LOG.info("Reader 端建表语句为：" + readerCreateSql);
        return readerCreateSql;
    }

    private static void createWriterTable(ConnectionInfo writerConnectionInfo, String readerCreateSql) {

        Matcher matcher = CREATE_TABLE_PATTERN.matcher(readerCreateSql);
        String readerTableName = null;
        if (matcher.find()) {
            readerTableName = matcher.group(1);
        }
        if (readerTableName == null) {
            throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR,
                    "找不到Reader端的表名.");
        }

        String replace = " IF NOT EXISTS " + writerConnectionInfo.getTablename();
        String writerCreateSql = readerCreateSql.replaceFirst(readerTableName, replace);

        LOG.info("Writer 端建表语句为：" + writerCreateSql);

        Statement statement = null;
        Connection writerConnection = DBUtil.getConnection(DataBaseType.MySql,
                writerConnectionInfo.getJdbcUrl(),
                writerConnectionInfo.getUsername(),
                writerConnectionInfo.getPassword());
        try {
            statement = writerConnection.createStatement();
            DBUtil.executeSqlWithoutResultSet(statement, writerCreateSql);
        } catch (SQLException e) {
            throw RdbmsException.asQueryException(DataBaseType.MySql, e, writerCreateSql, writerConnectionInfo.getTablename(), null);
        } finally {
            DBUtil.closeDBResources(null, statement, writerConnection);
        }
    }


    static class ConnectionInfo {
        private String jdbcUrl;
        private String username;
        private String password;
        private String tablename;


        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getTablename() {
            return tablename;
        }

        public void setTablename(String tablename) {
            this.tablename = tablename;
        }
    }
}