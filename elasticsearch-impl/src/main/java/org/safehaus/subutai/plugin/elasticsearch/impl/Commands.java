package org.safehaus.subutai.plugin.elasticsearch.impl;


import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


public class Commands
{

    public RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "service elasticsearch status" );
    }


    public RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "service elasticsearch start" );
    }


    public RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "service elasticsearch stop" );
    }


    public RequestBuilder getConfigureCommand( String clusterName )
    {
        return new RequestBuilder( String.format( ". /etc/profile && es-conf.sh cluster.name %s", clusterName ) );
    }


    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes install %s",
                ElasticsearchClusterConfiguration.PACKAGE_NAME ) ).withTimeout( 600 );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes purge %s",
                ElasticsearchClusterConfiguration.PACKAGE_NAME ) ).withTimeout( 300 );
    }


    public RequestBuilder getCheckInstallationCommand()
    {
        return new RequestBuilder(
                String.format( "dpkg -l | grep '^ii' | grep %s", Common.PACKAGE_PREFIX_WITHOUT_DASH ) );
    }
}
