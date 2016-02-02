package io.subutai.plugin.sqoop.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.sqoop.api.SqoopConfig;


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
        Set<EnvironmentContainerHost> nodes = null;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( CollectionUtil.isCollectionEmpty( nodes ) || nodes.size() < config.getNodes().size() )
        {
            throw new ClusterSetupException( "Fewer nodes found in the environment than expected" );
        }

        for ( EnvironmentContainerHost node : nodes )
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
        Iterator<EnvironmentContainerHost> it = nodes.iterator();
        while ( it.hasNext() )
        {
            EnvironmentContainerHost node = it.next();
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
                    else if ( !res.getStdOut().contains( hadoop_pack ) )
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
        Set<Host> hosts = getHosts( nodes );
        try
        {

            Map<Host, CommandResult> hostCommandResultMap =
                    commandUtil.executeParallel( new RequestBuilder( s ).withTimeout( 600 ), hosts );
            checkInstalled( hostCommandResultMap, hosts );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( e );
        }
        //        while ( it.hasNext() )
        //        {
        //            EnvironmentContainerHost node = it.next();
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


    public void checkInstalled( Map<Host, CommandResult> hostCommandResultMap, Set<Host> hosts )
            throws ClusterSetupException
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


    private Set<Host> getHosts( Set<EnvironmentContainerHost> containerHosts )
    {
        Set<Host> hosts = Sets.newHashSet();
        for ( EnvironmentContainerHost ch : containerHosts )
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
                host.execute( new RequestBuilder( CommandFactory.build( NodeOperationType.DESTROY, null ) ) );
            }
            catch ( CommandException e )
            {
                to.addLog( String.format( "Unable to execute uninstall command on node %s", host.getHostname() ) );
            }
        }
    }
}

