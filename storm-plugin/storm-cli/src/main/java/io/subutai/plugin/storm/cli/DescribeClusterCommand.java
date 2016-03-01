package io.subutai.plugin.storm.cli;


import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;


@Command( scope = "storm", name = "describe-cluster", description = "Shows the details of the storm cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Storm stormManager;


    protected Object doExecute()
    {
        StormClusterConfiguration config = stormManager.getCluster( clusterName );
        if ( config != null )
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Domain name: " ).append( config.getDomainName() ).append( "\n" );
            sb.append( "Nimbus node: " ).append( config.getNimbus() ).append( "\n" );
            sb.append( "Worker nodes: " ).append( config.getSupervisors() ).append( "\n" );
            System.out.println( sb.toString() );
        }
        else
        {
            System.out.println( "No clusters found..." );
        }

        return null;
    }


    public void setStormManager( final Storm stormManager )
    {
        this.stormManager = stormManager;
    }
}
