package io.subutai.plugin.flume.cli;


import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;


@Command( scope = "flume", name = "describe-cluster", description = "Shows the details of the Flume cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Flume flumeManager;


    protected Object doExecute()
    {
        FlumeConfig config = flumeManager.getCluster( clusterName );
        if ( config != null )
        {
            System.out.println( "Cluster name: " + config.getClusterName() + "\n" );
        }
        else
        {
            System.out.println( "No clusters found..." );
        }

        return null;
    }


    public void setFlumeManager( final Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }
}
