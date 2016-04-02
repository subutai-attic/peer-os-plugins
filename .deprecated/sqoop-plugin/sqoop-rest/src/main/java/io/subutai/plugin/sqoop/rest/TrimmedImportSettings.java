package io.subutai.plugin.sqoop.rest;


public class TrimmedImportSettings
{
    private String clusterName;
    private String connectionString;
    private String username;
    private String password;
    private boolean importAllTables;
    private String tableName;
    private String dataSourceType;
    private String hostname;


    public String getHostname()
    {
        return hostname;
    }


    public void setHostname( final String hostname )
    {
        this.hostname = hostname;
    }


    public String getDataSourceType()
    {
        return dataSourceType;
    }


    public void setDataSourceType( final String dataSourceType )
    {
        this.dataSourceType = dataSourceType;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getConnectionString()
    {
        return connectionString;
    }


    public void setConnectionString( final String connectionString )
    {
        this.connectionString = connectionString;
    }


    public String getUsername()
    {
        return username;
    }


    public void setUsername( final String username )
    {
        this.username = username;
    }


    public String getPassword()
    {
        return password;
    }


    public void setPassword( final String password )
    {
        this.password = password;
    }


    public boolean isImportAllTables()
    {
        return importAllTables;
    }


    public void setImportAllTables( final boolean importAllTables )
    {
        this.importAllTables = importAllTables;
    }


    public String getTableName()
    {
        return tableName;
    }


    public void setTableName( final String tableName )
    {
        this.tableName = tableName;
    }


    @Override
    public String toString()
    {
        return "TrimmedImportSettings{" +
                "clusterName='" + clusterName + '\'' +
                ", connectionString='" + connectionString + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", importAllTables=" + importAllTables +
                ", tableName='" + tableName + '\'' +
                ", dataSourceType='" + dataSourceType + '\'' +
                ", hostname='" + hostname + '\'' +
                '}';
    }
}
