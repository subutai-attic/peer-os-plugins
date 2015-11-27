package io.subutai.plugin.hive.cli;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hive.api.Hive;


/**
 * sample command : hive:check-cluster test \ {cluster name}
 */
@Command( scope = "hive", name = "check-cluster", description = "Command to check Hive cluster" )
public class CheckClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Hive hiveManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( CheckClusterCommand.class.getName() );


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Checking cluster nodes ... " );
        UUID checkUUID = hiveManager.statusCheck( clusterName, server );
        TrackerReader.checkStatus( tracker, checkUUID );
        return null;
    }


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( Hive hiveManager )
    {
        this.hiveManager = hiveManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
