package org.safehaus.subutai.plugin.mongodb.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "mongo", name = "start-cluster", description = "Starts cluster" )
public class StartClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false ) String clusterName = null;
    private Tracker tracker;
    private Mongo mongoManager;

    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = getMongoManager().startAllNodes( clusterName );
        System.out.println( "Start cluster operation is " + InstallClusterCommand.waitUntilOperationFinish(
                getTracker(), uuid ) + "." );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Mongo getMongoManager()
    {
        return mongoManager;
    }


    public void setMongoManager( final Mongo mongoManager )
    {
        this.mongoManager = mongoManager;
    }
}
