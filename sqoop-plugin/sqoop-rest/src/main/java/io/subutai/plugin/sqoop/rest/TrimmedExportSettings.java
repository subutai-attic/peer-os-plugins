package io.subutai.plugin.sqoop.rest;


public class TrimmedExportSettings
{
    private String clusterName;
    private String connectionString;
    private String username;
    private String password;
    private String tableName;
    private String hostname;
    private String hdfsFilePath;


    public String getHdfsFilePath()
    {
        return hdfsFilePath;
    }


    public void setHdfsFilePath( final String hdfsFilePath )
    {
        this.hdfsFilePath = hdfsFilePath;
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


    public String getTableName()
    {
        return tableName;
    }


    public void setTableName( final String tableName )
    {
        this.tableName = tableName;
    }


    public String getHostname()
    {
        return hostname;
    }


    public void setHostname( final String hostname )
    {
        this.hostname = hostname;
    }
}
