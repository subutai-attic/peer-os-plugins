package io.subutai.plugin.galera.impl;


import io.subutai.common.command.RequestBuilder;


public class Commands
{
    public static RequestBuilder getStartMySql()
    {
        return new RequestBuilder( "service mysql start" );
    }


    public static RequestBuilder getStopMySql()
    {
        return new RequestBuilder( "service mysql stop" );
    }


    public static RequestBuilder getStatusMySql()
    {
        return new RequestBuilder( "service mysql status" );
    }


    public static RequestBuilder getBootstrapMySql()
    {
        return new RequestBuilder( "service mysql bootstrap" );
    }


    public static RequestBuilder getSetClusterAddress( String ips )
    {
        return new RequestBuilder( String.format( "bash /etc/mysql/conf.d/galera-conf.sh cluster-address %s", ips ) );
    }


    public static RequestBuilder getSetNodeAddress( String ip )
    {
        return new RequestBuilder( String.format( "bash /etc/mysql/conf.d/galera-conf.sh node-address %s", ip ) );
    }


    public static RequestBuilder getCleanupConfigCommand()
    {
        return new RequestBuilder( "rm /etc/mysql/conf.d/cluster.cnf ; cp /etc/mysql/conf.d/cluster.cnf.example /etc/mysql/conf.d/cluster.cnf" );
    }
}
