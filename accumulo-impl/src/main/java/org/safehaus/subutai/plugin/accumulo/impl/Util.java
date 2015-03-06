package org.safehaus.subutai.plugin.accumulo.impl;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Util
{

    private static final Logger LOG = LoggerFactory.getLogger( Util.class.getName() );


    /**
     * @param config accumulo cluster configuration object
     * @param environment environment
     *
     * @return set of {@link org.safehaus.subutai.common.peer.Host} objects
     */
    public static Set<Host> getHosts( AccumuloClusterConfig config, Environment environment )
    {
        Set<Host> hosts = new HashSet<>();
        for ( UUID uuid : config.getAllNodes() )
        {
            try
            {
                hosts.add( environment.getContainerHostById( uuid ) );
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
