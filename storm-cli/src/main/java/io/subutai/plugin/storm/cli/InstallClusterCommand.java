package io.subutai.plugin.storm.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "storm", name = "install-cluster", description = "Command to install storm cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "storm cluster "
            + "name" ) String clusterName;

    @Argument( index = 1, name = "environmentId", description = "The environment id",
            required = true, multiValued = false ) String environmentId;

    @Argument( index = 2, name = "nimbus", description = "The uuid of nimbus node", required = true,
            multiValued = false ) String nimbus;

    @Argument( index = 3, name = "supervisors", description = "The uuid list of rsupervisors nodes",
            required = true, multiValued = false ) String supervisors[] = null;

    private Tracker tracker;
    private Storm stormManager;


    protected Object doExecute()
    {
        StormClusterConfiguration config = new StormClusterConfiguration();
        config.setClusterName( clusterName );
        config.setExternalZookeeper( false );
        config.setEnvironmentId( UUID.fromString( environmentId ) );
        config.setNimbus( UUID.fromString( nimbus ) );
        Set<UUID> supervisorNodes = new HashSet<>();
        for ( String node : supervisors )
        {
            supervisorNodes.add( UUID.fromString( node ) );
        }
        config.setSupervisors( supervisorNodes );
        UUID uuid = stormManager.installCluster( config );
        System.out.println(
                "Install operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
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
