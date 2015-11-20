package io.subutai.plugin.cassandra.webui;


import io.subutai.webui.api.WebuiModule;


public class CassandraWebModule implements WebuiModule
{
    public static String NAME = "Cassandra";
    public static String IMG = "plugins/cassandra/cassandra.png";

    public CassandraWebModule()
    {

    }

    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getModuleInfo()
    {
        return String.format( "{'img' : '%s', 'name' : '%s'}", IMG, NAME );
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( "" );
    }


    public void init()
    {

    }

    public void destroy()
    {

    }
}
