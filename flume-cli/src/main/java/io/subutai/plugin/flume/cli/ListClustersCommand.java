package io.subutai.plugin.flume.cli;


import java.util.List;

import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "flume", name = "list-clusters", description = "mydescription" )
public class ListClustersCommand extends OsgiCommandSupport
{

    @Override
    protected Object doExecute()
    {

        List<FlumeConfig> configList = flumeManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( FlumeConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Flume clusters" );
        }

        return null;
    }


    private Flume flumeManager;


    public Flume getFlumeManager()
    {
        return flumeManager;
    }


    public void setFlumeManager( Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }

}
