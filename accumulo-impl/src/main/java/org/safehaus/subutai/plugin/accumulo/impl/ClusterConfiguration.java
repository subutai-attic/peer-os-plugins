package org.safehaus.subutai.plugin.accumulo.impl;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Configures Accumulo Cluster
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ClusterConfiguration.class );
    private TrackerOperation trackerOperation;
    private AccumuloImpl accumuloManager;


    public ClusterConfiguration( final AccumuloImpl accumuloManager, final TrackerOperation trackerOperation )
    {
        Preconditions.checkNotNull( accumuloManager, "Accumulo Manager is null" );
        Preconditions.checkNotNull( trackerOperation, "Product Operation is null" );
        this.trackerOperation = trackerOperation;
        this.accumuloManager = accumuloManager;
    }


    @Override
    public void configureCluster( final ConfigBase configBase, final Environment environment )
            throws ClusterConfigurationException
    {
        AccumuloClusterConfig accumuloClusterConfig = ( AccumuloClusterConfig ) configBase;
        ZookeeperClusterConfig zookeeperClusterConfig =
                accumuloManager.getZkManager().getCluster( accumuloClusterConfig.getZookeeperClusterName() );

        trackerOperation.addLog( "Configuring cluster..." );
        ContainerHost master = getHost( environment, accumuloClusterConfig.getMasterNode() );
        ContainerHost gc = getHost( environment, accumuloClusterConfig.getGcNode() );
        ContainerHost monitor = getHost( environment, accumuloClusterConfig.getMonitor() );


        /** configure cluster */
        for ( UUID uuid : accumuloClusterConfig.getAllNodes() )
        {
            ContainerHost host = getHost( environment, uuid );

            // configure master node
            executeCommand( host, Commands.getAddMasterCommand( master.getHostname() ) );

            // configure GC node
            executeCommand( host, Commands.getAddGCCommand( gc.getHostname() ) );

            // configure monitor node
            executeCommand( host, Commands.getAddMonitorCommand( monitor.getHostname() ) );

            // configure tracers
            executeCommand( host, Commands.getAddTracersCommand(
                    serializeSlaveNodeNames( environment, accumuloClusterConfig.getTracers() ) ) );

            // configure slaves
            executeCommand( host, Commands.getAddSlavesCommand(
                    serializeSlaveNodeNames( environment, accumuloClusterConfig.getSlaves() ) ) );

            // configure zookeeper
            executeCommand( host, Commands.getBindZKClusterCommand( serializeZKNodeNames( zookeeperClusterConfig ) ) );
        }

        // init accumulo instance
        executeCommand( master, Commands.getInitCommand( accumuloClusterConfig.getInstanceName(),
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


    private String serializeSlaveNodeNames( Environment environment, Set<UUID> slaveNodes )
    {
        StringBuilder slavesSpaceSeparated = new StringBuilder();
        for ( UUID tracer : slaveNodes )
        {
            slavesSpaceSeparated.append( getHost( environment, tracer ).getHostname() ).append( " " );
        }
        return slavesSpaceSeparated.toString();
    }


    private String serializeZKNodeNames( ZookeeperClusterConfig zookeeperClusterConfig )
    {
        Environment environment = null;
        try
        {
            environment = accumuloManager.getEnvironmentManager()
                                         .findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.",
                    zookeeperClusterConfig.getEnvironmentId().toString() );
        }
        Set<UUID> zkNodes = zookeeperClusterConfig.getNodes();
        StringBuilder zkNodesCommaSeparated = new StringBuilder();
        for ( UUID zkNode : zkNodes )
        {
            zkNodesCommaSeparated.append( getHost( environment, zkNode ).getHostname() ).append( ":2181," );
        }
        zkNodesCommaSeparated.delete( zkNodesCommaSeparated.length() - 1, zkNodesCommaSeparated.length() );
        return zkNodesCommaSeparated.toString();
    }


    private void executeCommand( ContainerHost host, RequestBuilder commandBuilder )
    {
        try
        {
            host.execute( commandBuilder );
        }
        catch ( CommandException e )
        {
            LOGGER.error( "Error executing command.", e );
            trackerOperation.addLogFailed( "Error executing command. " + e.getMessage() );
        }
    }


    private ContainerHost getHost( Environment environment, UUID nodeId )
    {
        try
        {
            return environment.getContainerHostById( nodeId );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg =
                    String.format( "Container host with id: %s doesn't exists in environment: %s", nodeId.toString(),
                            environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
    }
}
