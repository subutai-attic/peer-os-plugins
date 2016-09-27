package io.subutai.plugin.solr.impl;


import io.subutai.common.command.RequestBuilder;


public class Commands
{
    public static final String CREATE_CONFIG_FILE = "touch /opt/zookeeper/conf/zoo.cfg";
    public static final String REMOVE_SNAPS_COMMAND = "rm -rf /var/lib/zookeeper/data/version-2/*";


    public static String getConfigureClusterCommand( String zooCfgFileContents, String zooCfgFilePath, int id )
    {
        return String
                .format( "bash /opt/zookeeper/conf/zookeeper-setID.sh %s && echo '%s' > %s", id, zooCfgFileContents,
                        zooCfgFilePath );
    }


    public static RequestBuilder getStartSolrCommand( final String hostnames, final String hostname )
    {
        return new RequestBuilder( String.format( "/opt/solr/bin/solr start -c -z %s -h %s", hostnames, hostname ) );
    }


    public static RequestBuilder getStopSolrCommand( final String hostnames, final String hostname )
    {
        return new RequestBuilder( String.format( "/opt/solr/bin/solr stop -c -z %s -h %s", hostnames, hostname ) );
    }


    public static RequestBuilder getStartZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh start" );
    }


    public static RequestBuilder getStopZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh stop" );
    }


    public static RequestBuilder getSolrStatusCommand()
    {
        return new RequestBuilder( "jps" );
    }


    public static RequestBuilder getRestartZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh restart" );
    }


    public static String getResetClusterConfigurationCommand( String zooCfgFileContents, String zooCfgFilePath )
    {
        return String.format( "echo '%s' > %s", zooCfgFileContents, zooCfgFilePath );
    }
}
