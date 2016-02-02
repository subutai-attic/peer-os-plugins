package io.subutai.plugin.hipi.cli;


import java.util.List;

import io.subutai.plugin.hipi.api.Hipi;
import io.subutai.plugin.hipi.api.HipiConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "hipi", name = "list-clusters", description = "mydescription" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Hipi hipiManager;


    protected Object doExecute()
    {
        List<HipiConfig> configList = hipiManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( HipiConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Hipi cluster" );
        }

        return null;
    }


    public Hipi getHipiManager()
    {
        return hipiManager;
    }


    public void setHipiManager( Hipi hipiManager )
    {
        this.hipiManager = hipiManager;
    }
}
