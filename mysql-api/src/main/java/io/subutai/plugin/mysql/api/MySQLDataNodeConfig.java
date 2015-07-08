package io.subutai.plugin.mysql.api;


/**
 * Created by tkila on 5/11/15.
 */
public class MySQLDataNodeConfig
{
    //
    private static final String DATA_NODE = "[mysqld]";

    //run NDB Storage Engine
    private static final String NDB_STRING = "ndbcluster";
    private String dataDir = "/usr/local/mysql/data";

    private String managerHost = "";


    public MySQLDataNodeConfig( String managerHost )
    {
        this.managerHost = managerHost;
    }


    public void setManagerHost( final String managerHost )
    {
        this.managerHost = managerHost;
    }


    public static String getDataNode()
    {
        return DATA_NODE;
    }


    public static String getNdbString()
    {
        return NDB_STRING;
    }


    public String getManagerHost()
    {
        return managerHost;
    }


    public String getDataDir()
    {
        return dataDir;
    }


    public void setDataDir( final String dataDir )
    {
        this.dataDir = dataDir;
    }


    public String exportConfig()
    {
        StringBuilder config = new StringBuilder();
        config.append( DATA_NODE + "\n" );
        config.append( NDB_STRING + "\n" );
        //config.append( "ndb-connectstring=" + managerHost +"\n");
        config.append( "[mysql_cluster]" + "\n" );
        config.append( "ndb-connectstring=" + managerHost + "\n" );

        return config.toString();
    }
}
