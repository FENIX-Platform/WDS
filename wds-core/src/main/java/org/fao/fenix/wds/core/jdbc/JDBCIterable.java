package org.fao.fenix.wds.core.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.constant.SQL;
import org.fao.fenix.wds.core.exception.WDSException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a>
 */
public class JDBCIterable implements Iterator<List<String>> {

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    private boolean hasNext;

    private int columns;

    public void query(DatasourceBean db, String sql) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        switch (db.getDriver()) {
            case POSTGRESQL     : queryPostgreSQL(db, sql); break;
            case SQLSERVER2000  : querySQLServer(db, sql); break;
            default             : throw new WDSException(db.getDriver().name() + " driver has not been implemented (yet).");
        }
    }

    public void querySQLServer(DatasourceBean db, String sql) throws IllegalAccessException, InstantiationException, SQLException {

        // Clean the query
        validate(sql);
        sql = sql.replaceAll("-", "_");

        System.out.println(db.getUrl());
        System.out.println(db.getUsername());
        System.out.println(db.getPassword());

        // Open connections
        SQLServerDriver.class.newInstance();
        this.setConnection(DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword()));
        this.setStatement(this.getConnection().createStatement());
        this.getStatement().executeQuery(sql);
        this.setResultSet(this.getStatement().getResultSet());

        this.setColumns(this.getResultSet().getMetaData().getColumnCount());

    }

    public void queryPostgreSQL(DatasourceBean db, String sql) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {

        // Clean the query
        if (!db.getId().equalsIgnoreCase("STAGINGAREA"))
            validate(sql);

        // Open connections
        Class.forName("org.postgresql.Driver");
        this.setConnection(DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword()));
        this.setStatement(this.getConnection().createStatement());
        this.getStatement().executeQuery(sql);
        this.setResultSet(this.getStatement().getResultSet());

    }

    @Override
    public boolean hasNext() {
        return this.isHasNext();
    }

    @Override
    public List<String> next() {

        List<String> l = null;

        if (this.isHasNext()) {
            l = new ArrayList<String>();
            try {
                for (int i = 1 ; i <= this.getResultSet().getMetaData().getColumnCount() ; i++)
                    l.add(this.getResultSet().getString(i).trim());
                this.setHasNext(this.getResultSet().next());
            } catch(SQLException ignored) {

            }
        }

        if (!this.isHasNext()) {
            try {
                this.getResultSet().close();
                this.getStatement().close();
                this.getConnection().close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return l;
    }

    @Override
    public void remove() {

    }

    public void validate(String sql) throws WDSException {
        if (sql.toUpperCase().contains(SQL.INSERT.name()))
            throw new WDSException(SQL.INSERT.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.UPDATE.name()) && !sql.toUpperCase().contains(SQL.DATEUPDATE.name()))
            throw new WDSException(SQL.UPDATE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.DELETE.name()))
            throw new WDSException(SQL.DELETE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.CREATE.name()) && !sql.toUpperCase().contains(SQL.CREATEDDATE.name()))
            throw new WDSException(SQL.CREATE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.DROP.name()))
            throw new WDSException(SQL.DROP.name() + " is NOT allowed.");
    }

    public ResultSet getResultSet() {
        return this.resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
        try {
            this.setHasNext(this.getResultSet().next());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public int getColumns() {
        return columns;
    }

    public void setColumns(int columns) {
        this.columns = columns;
    }

}