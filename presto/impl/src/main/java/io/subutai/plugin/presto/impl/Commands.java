package io.subutai.plugin.presto.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.plugin.presto.api.PrestoClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + "presto2";


    public RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
    }


    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 2000 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 600 );
    }


    public static RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }


    public RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "/opt/presto/bin/launcher start" );
    }


    public RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "/opt/presto/bin/launcher stop" );
    }


    public RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "jps" );
    }


    public RequestBuilder getSetCoordinatorCommand( ContainerHost coordinatorNode )
    {
        String s = String.format( "presto-config.sh coordinator %s", coordinatorNode.getHostname() );
        return new RequestBuilder( s );
    }


    public RequestBuilder getSetWorkerCommand( ContainerHost node )
    {
        String s = String.format( "presto-config.sh worker %s", node.getHostname() );
        return new RequestBuilder( s );
    }


    public static RequestBuilder getCopyCoordinatorConfigCommand()
    {
        return new RequestBuilder(
                "cp /opt/presto/etc/config.properties-coordinator /opt/presto/etc/config.properties" );
    }


    public static RequestBuilder getCopyWorkerConfigCommand()
    {
        return new RequestBuilder( "cp /opt/presto/etc/config.properties-worker /opt/presto/etc/config.properties" );
    }


    public static RequestBuilder getSetNodeIdCommand( String nodeId )
    {
        return new RequestBuilder( String.format(
                "sed -i -e 's/ffffffff-ffff-ffff-ffff-ffffffffffff/%s/g' /opt/presto/etc/node.properties", nodeId ) );
    }


    public static RequestBuilder getSetClusterNameCommand( String clusterName )
    {
        return new RequestBuilder(
                String.format( "sed -i -e 's/production/%s/g' /opt/presto/etc/node.properties", clusterName ) );
    }


    public static RequestBuilder getConfigureUriCommand( String hostname )
    {
        return new RequestBuilder(
                String.format( "sed -i -e 's/example.com/%s/g' /opt/presto/etc/config.properties", hostname ) );
    }
}
