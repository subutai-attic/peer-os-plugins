package io.subutai.plugin.elasticsearch.impl;


import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


public class Commands
{

    public static RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "service elasticsearch status" );
    }


    public static RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "service elasticsearch start" );
    }


    public static RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "service elasticsearch stop" );
    }


    public static RequestBuilder getRestartCommand()
    {
        return new RequestBuilder( "service elasticsearch restart" );
    }


    RequestBuilder setClusterNameCommand( String clusterName )
    {
        return new RequestBuilder( String.format( "bash /etc/elasticsearch/scripts/es-conf.sh cluster.name %s", clusterName ) );
    }


    RequestBuilder setNodeNameCommand( String nodeName )
    {
        return new RequestBuilder( String.format( "bash /etc/elasticsearch/scripts/es-conf.sh node.name %s", nodeName ) );
    }


    RequestBuilder setNetworkHostCommand( String host )
    {
        return new RequestBuilder( String.format( "bash /etc/elasticsearch/scripts/es-conf.sh network.host %s", host ) );
    }


    RequestBuilder setUnicastHostsCommand( String hosts )
    {
        return new RequestBuilder( String.format( "bash /etc/elasticsearch/scripts/es-conf.sh unicast.hosts %s", hosts ) );
    }


    RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes install %s",
                ElasticsearchClusterConfiguration.PACKAGE_NAME ) ).withTimeout( 600 );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes purge %s",
                ElasticsearchClusterConfiguration.PACKAGE_NAME ) ).withTimeout( 300 );
    }


    RequestBuilder getCheckInstallationCommand()
    {
        return new RequestBuilder(
                String.format( "dpkg -l | grep '^ii' | grep %s", Common.PACKAGE_PREFIX_WITHOUT_DASH ) );
    }
}
