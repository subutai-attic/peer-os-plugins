package io.subutai.plugin.accumulo.impl;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;


public class Util
{

    private static final Logger LOG = LoggerFactory.getLogger( Util.class.getName() );


    /**
     * @param config accumulo cluster configuration object
     * @param environment environment
     *
     * @return set of {@link io.subutai.common.peer.Host} objects
     */
    public static Set<Host> getHosts( AccumuloClusterConfig config, Environment environment )
    {
        Set<Host> hosts = new HashSet<>();
        for ( String id : config.getAllNodes() )
        {
            try
            {
                hosts.add( environment.getContainerHostById( id ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        return hosts;
    }


    /**
     * @param resultMap command result map
     * @param hosts set of hosts
     *
     * @return boolean value if all commands are run successfully in given resultMap
     */
    public static boolean isAllSuccessful( Map<Host, CommandResult> resultMap, Set<Host> hosts )
    {
        boolean allSuccess = true;
        for ( Host host : hosts )
        {
            if ( !resultMap.get( host ).hasSucceeded() )
            {
                allSuccess = false;
            }
        }
        return allSuccess;
    }


    /**
     * Checks results if products is installed on all given hosts.
     *
     * @param resultMap command result map
     * @param hosts set of hosts
     * @param productName product name
     */
    public static boolean isProductInstalledOnAllNodes( Map<Host, CommandResult> resultMap, Set<Host> hosts,
                                                        String productName )
    {
        boolean installedOnAllNodes = true;
        for ( Host host : hosts )
        {
            if ( !resultMap.get( host ).getStdOut().toLowerCase().contains( productName.toLowerCase() ) )
            {
                installedOnAllNodes = false;
            }
        }
        return installedOnAllNodes;
    }


    public static boolean isProductNotInstalledOnAllNodes( Map<Host, CommandResult> resultMap, Set<Host> hosts,
                                                           String productName )
    {
        boolean installedOnAllNodes = false;
        for ( Host host : hosts )
        {
            if ( resultMap.get( host ).getStdOut().toLowerCase().contains( productName.toLowerCase() ) )
            {
                installedOnAllNodes = true;
            }
        }
        return installedOnAllNodes;
    }


    public static void waitUntilOperationFinish( AccumuloImpl manager, UUID uuid )
    {
        long start = System.currentTimeMillis();

        while ( !Thread.interrupted() )
        {
            TrackerOperationView po =
                    manager.getTracker().getTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > 120 * 1000 )
            {
                break;
            }
        }
    }


    public static CommandResult executeCommand( ContainerHost containerHost, RequestBuilder commandRequest )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( commandRequest );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", e );
        }
        return result;
    }
}
