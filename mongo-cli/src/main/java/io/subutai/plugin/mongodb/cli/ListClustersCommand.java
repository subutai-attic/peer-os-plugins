package io.subutai.plugin.mongodb.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.mongodb.api.Mongo;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;


/**
 * Displays the last log entries
 */
@Command( scope = "mongo", name = "list-clusters", description = "mydescription" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Mongo mongoManager;


    public void setMongoManager( Mongo mongoManager )
    {
        this.mongoManager = mongoManager;
    }


    protected Object doExecute()
    {
        List<MongoClusterConfig> configList = mongoManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( MongoClusterConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Mongo cluster" );
        }

        return null;
    }
}
