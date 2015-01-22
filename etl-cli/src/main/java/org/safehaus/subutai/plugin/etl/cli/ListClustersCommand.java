package org.safehaus.subutai.plugin.etl.cli;


import java.util.List;

import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.etl.api.Sqoop;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Displays the last log entries
 */
@Command( scope = "sqoop", name = "list-clusters", description = "mydescription" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Sqoop sqoopManager;


    public Sqoop getSqoopManager()
    {
        return sqoopManager;
    }


    public void setSqoopManager( Sqoop sqoopManager )
    {
        this.sqoopManager = sqoopManager;
    }


    @Override
    protected Object doExecute()
    {
        List<ETLConfig> configList = sqoopManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( ETLConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Sqoop cluster" );
        }

        return null;
    }
}
