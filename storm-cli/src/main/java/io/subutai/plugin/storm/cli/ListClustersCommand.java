package io.subutai.plugin.storm.cli;


import java.util.List;

import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "storm", name = "list-clusters", description = "Lists clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Storm stormManager;


    protected Object doExecute()
    {
        List<StormClusterConfiguration> configs = stormManager.getClusters();
        StringBuilder sb = new StringBuilder();

        if ( configs.isEmpty() )
        {
            System.out.println( "No clusters found" );
            return null;
        }

        for ( StormClusterConfiguration config : configs )
        {
            sb.append( config.getClusterName() ).append( "\n" );
        }

        System.out.println( sb.toString() );

        return null;
    }


    public Storm getStormManager()
    {
        return stormManager;
    }


    public void setStormManager( final Storm stormManager )
    {
        this.stormManager = stormManager;
    }
}
