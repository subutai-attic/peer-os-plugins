package io.subutai.plugin.accumulo.impl;


import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * Configures Accumulo Cluster
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ClusterConfiguration.class );
    private TrackerOperation trackerOperation;
    private AccumuloImpl accumuloManager;
    private CommandUtil commandUtil;


    public ClusterConfiguration( final AccumuloImpl accumuloManager, final TrackerOperation trackerOperation )
    {
        Preconditions.checkNotNull( accumuloManager, "Accumulo Manager is null" );
        Preconditions.checkNotNull( trackerOperation, "Product Operation is null" );
        this.trackerOperation = trackerOperation;
        this.accumuloManager = accumuloManager;
        this.commandUtil = new CommandUtil();
    }


    @Override
    public void configureCluster( final ConfigBase configBase, final Environment environment )
            throws ClusterConfigurationException
    {
        AccumuloClusterConfig accumuloClusterConfig = ( AccumuloClusterConfig ) configBase;
        ZookeeperClusterConfig zookeeperClusterConfig =
                accumuloManager.getZkManager().getCluster( accumuloClusterConfig.getZookeeperClusterName() );

        trackerOperation.addLog( "Configuring cluster..." );
        EnvironmentContainerHost master = getHost( environment, accumuloClusterConfig.getMasterNode() );
        EnvironmentContainerHost gc = getHost( environment, accumuloClusterConfig.getGcNode() );
        EnvironmentContainerHost monitor = getHost( environment, accumuloClusterConfig.getMonitor() );


        // clear configuration files
        executeCommandOnAllContainer( accumuloClusterConfig.getAllNodes(),
                Commands.getClearMastersFileCommand( "masters" ), environment );
        executeCommandOnAllContainer( accumuloClusterConfig.getAllNodes(),
                Commands.getClearSlavesFileCommand( "slaves" ), environment );
        executeCommandOnAllContainer( accumuloClusterConfig.getAllNodes(),
                Commands.getClearMastersFileCommand( "tracers" ), environment );
        executeCommandOnAllContainer( accumuloClusterConfig.getAllNodes(), Commands.getClearMastersFileCommand( "gc" ),
                environment );
        executeCommandOnAllContainer( accumuloClusterConfig.getAllNodes(),
                Commands.getClearMastersFileCommand( "monitor" ), environment );

        /** configure cluster */
        Set<Host> hostSet = Util.getHosts( accumuloClusterConfig, environment );
        try
        {
            // configure master node
            commandUtil.executeParallel( Commands.getAddMasterCommand( master.getHostname() ), hostSet );

            // configure GC node
            commandUtil.executeParallel( Commands.getAddGCCommand( gc.getHostname() ), hostSet );

            // configure monitor node
            commandUtil.executeParallel( Commands.getAddMonitorCommand( monitor.getHostname() ), hostSet );

            // configure tracers
            commandUtil.executeParallel( Commands.getAddTracersCommand(
                    serializeSlaveNodeNames( environment, accumuloClusterConfig.getTracers() ) ), hostSet );

            // configure slaves
            commandUtil.executeParallel( Commands.getAddSlavesCommand(
                    serializeSlaveNodeNames( environment, accumuloClusterConfig.getSlaves() ) ), hostSet );

            // configure zookeeper
            commandUtil.executeParallel(
                    Commands.getBindZKClusterCommand( serializeZKNodeNames( zookeeperClusterConfig ) ), hostSet );
        }
        catch ( CommandException e )
        {
            LOGGER.error( "Error while running configuration commands", e );
            e.printStackTrace();
            return;
        }

        // init accumulo instance
        Util.executeCommand( master, Commands.getInitCommand( accumuloClusterConfig.getInstanceName(),
                accumuloClusterConfig.getPassword() ) );


        accumuloClusterConfig.setEnvironmentId( environment.getId() );
        accumuloManager.getPluginDAO()
                       .saveInfo( AccumuloClusterConfig.PRODUCT_KEY, accumuloClusterConfig.getClusterName(),
                               accumuloClusterConfig );

        // start cluster
        // trackerOperation.addLog( "Starting cluster ..." );
        // executeCommand( master, Commands.startCommand );

        trackerOperation.addLogDone( AccumuloClusterConfig.PRODUCT_KEY + " cluster data saved into database" );
    }


    private String serializeSlaveNodeNames( Environment environment, Set<String> slaveNodes )
    {
        StringBuilder slavesSpaceSeparated = new StringBuilder();
        for ( String tracer : slaveNodes )
        {
            slavesSpaceSeparated.append( getHost( environment, tracer ).getHostname() ).append( " " );
        }
        return slavesSpaceSeparated.toString();
    }


    public String serializeZKNodeNames( ZookeeperClusterConfig zookeeperClusterConfig )
    {
        Environment environment = null;
        try
        {
            environment = accumuloManager.getEnvironmentManager()
                                         .loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.",
                    zookeeperClusterConfig.getEnvironmentId() );
        }
        Set<String> zkNodes = zookeeperClusterConfig.getNodes();
        StringBuilder zkNodesCommaSeparated = new StringBuilder();
        for ( String zkNode : zkNodes )
        {
            zkNodesCommaSeparated.append( getHost( environment, zkNode ).getHostname() ).append( ":2181," );
        }
        zkNodesCommaSeparated.delete( zkNodesCommaSeparated.length() - 1, zkNodesCommaSeparated.length() );
        return zkNodesCommaSeparated.toString();
    }


    private EnvironmentContainerHost getHost( Environment environment, String nodeId )
    {
        try
        {
            return environment.getContainerHostById( nodeId );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with id: %s doesn't exists in environment: %s", nodeId,
                    environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
    }


    public static void executeCommandOnAllContainer( Set<String> allUUIDs, RequestBuilder command,
                                                     Environment environment )
    {
        CommandUtil commandUtil = new CommandUtil();
        try
        {
            Set<Host> hosts = new HashSet<>();
            for ( String id : allUUIDs )
            {
                hosts.add( environment.getContainerHostById( id ) );
            }
            try
            {
                commandUtil.executeParallel( command, hosts );
            }
            catch ( CommandException e )
            {
                LOGGER.error( "Error while executing commands in parallel", e );
                e.printStackTrace();
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Could not get all containers", e );
            e.printStackTrace();
        }
    }
}
