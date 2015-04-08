package org.safehaus.subutai.plugin.sqoop.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.hostregistry.api.HostRegistry;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

import com.google.common.collect.Sets;


class SetupStrategyOverHadoop extends SqoopSetupStrategy
{

    CommandUtil commandUtil;
    public SetupStrategyOverHadoop( SqoopImpl manager, SqoopConfig config, Environment env, TrackerOperation po )
    {
        super( manager, config, env, po );
        commandUtil = new CommandUtil();
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {

        checkConfig();

        //check if nodes are connected
        Set<ContainerHost> nodes = null;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( nodes.size() < config.getNodes().size() )
        {
            throw new ClusterSetupException( "Fewer nodes found in the environment than expected" );
        }

        for ( ContainerHost node : nodes )
        {
            if ( !node.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }
        }

        HadoopClusterConfig hc = manager.hadoopManager.getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }

        if ( !hc.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }
        config.setHadoopNodes( new HashSet<>( hc.getAllNodes() ) );

        // check if already installed
        String s = CommandFactory.build( NodeOperationType.STATUS, null );
        String hadoop_pack = Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase();
        Iterator<ContainerHost> it = nodes.iterator();
        while ( it.hasNext() )
        {
            ContainerHost node = it.next();
            try
            {
                CommandResult res = node.execute( new RequestBuilder( s ) );
                if ( res.hasSucceeded() )
                {
                    if ( res.getStdOut().contains( CommandFactory.PACKAGE_NAME ) )
                    {
                        to.addLog( String.format( "Node %s has already Sqoop installed.", node.getHostname() ) );
                        it.remove();
                    }
                    else if ( ! res.getStdOut().contains( hadoop_pack ) )
                    {
                        throw new ClusterSetupException( "Hadoop not installed on node " + node.getHostname() );
                    }
                }
                else
                {
                    throw new ClusterSetupException( "Failed to check installed packges on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }
        if ( nodes.isEmpty() )
        {
            throw new ClusterSetupException( "No nodes to install Sqoop" );
        }

        // installation
        s = CommandFactory.build( NodeOperationType.INSTALL, null );
        it = nodes.iterator();
        Set<Host> hosts = getHosts( nodes );
        try
        {

            Map<Host, CommandResult> hostCommandResultMap =  commandUtil.executeParallel( new RequestBuilder( s ).withTimeout( 600 ), hosts );
            checkInstalled( hostCommandResultMap, hosts );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( e );
        }
//        while ( it.hasNext() )
//        {
//            ContainerHost node = it.next();
//            try
//            {
//                CommandResult res = node.execute( new RequestBuilder( s ).withTimeout( 600 ) );
//                checkInstalled( node, res );
//            }
//            catch ( CommandException ex )
//            {
//                throw new ClusterSetupException( ex );
//            }
//        }

        to.addLog( "Saving to db..." );
        config.setEnvironmentId( environment.getId() );

        boolean saved = manager.getPluginDao().saveInfo( SqoopConfig.PRODUCT_KEY, config.getClusterName(), config );
        if ( saved )
        {
            to.addLog( "Installation info successfully saved" );
            configure();
        }
        else
        {
            throw new ClusterSetupException( "Failed to save installation info" );
        }

        return config;
    }

    public void checkInstalled( Map<Host, CommandResult> hostCommandResultMap, Set<Host> hosts ) throws ClusterSetupException
    {
        String okString = "install ok installed";
        boolean status = true;
        CommandResult currentResult = null;
        Host currentContainer = null;
        //nodes which already sqoop installed on.
        Set<Host> nodes = Sets.newHashSet();
        for ( Host host : hosts )
        {
            currentResult = hostCommandResultMap.get( host );
            currentContainer = host;
            CommandResult statusResult;
            try
            {
                statusResult = host.execute( CommandFactory.getCheckInstalledCommand( CommandFactory.PACKAGE_NAME ) );
            }
            catch ( CommandException e )
            {
                status = false;
                break;
            }

            if ( !( currentResult.hasSucceeded() && statusResult.getStdOut().contains( okString ) ) )
            {
                Set<Host> node = Sets.newHashSet();
                node.add( host );
                uninstallProductOnNode( node );
                status = false;
                break;
            }
            nodes.add( host );
        }
        if ( !status )
        {
            uninstallProductOnNode( nodes );
            to.addLogFailed(
                    String.format( "Couldn't install product on container %s:", currentContainer.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", currentContainer.getHostname(),
                    currentResult.hasCompleted() ? currentResult.getStdErr() : "Command timed out" ) );
        }
    }

    private Set<Host> getHosts( Set<ContainerHost> containerHosts )
    {
        Set<Host> hosts = Sets.newHashSet();
        for( ContainerHost ch : containerHosts)
        {
            hosts.add( ch );
        }
        return hosts;
    }

    private void uninstallProductOnNode( Set<Host> hosts )
    {

        for ( Host host : hosts )
        {
            try
            {
                host.execute( CommandFactory.getConfigureCommand() );
                host.execute( new RequestBuilder( CommandFactory.build( NodeOperationType.DESTROY , null) ) );
            }
            catch ( CommandException e )
            {
                to.addLog( String.format( "Unable to execute uninstall command on node %s", host.getHostname() ) );
            }
        }
    }
}

