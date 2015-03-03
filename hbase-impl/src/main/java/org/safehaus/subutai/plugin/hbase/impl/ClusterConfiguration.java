package org.safehaus.subutai.plugin.hbase.impl;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );
    private TrackerOperation po;
    private HBaseImpl hBase;
    private Hadoop hadoop;


    public ClusterConfiguration( final TrackerOperation operation, final HBaseImpl hBase, final Hadoop hadoop )
    {
        this.po = operation;
        this.hBase = hBase;
        this.hadoop = hadoop;
    }


    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        HBaseConfig config = ( HBaseConfig ) configBase;
        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );

        // clear configuration files
        clearConfigurationFiles( config.getAllNodes(), environment );

        // configure master
        po.addLog( "Configuring hmaster... !" );
        UUID hmaster = config.getHbaseMaster();
        ContainerHost hmasterContainerHost;
        try
        {
            hmasterContainerHost = environment.getContainerHostById( hmaster );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting hmaster container host.", e );
            po.addLogFailed( "Error getting hmaster container host." );
            return;
        }
        ContainerHost namenode;
        try
        {
            namenode = environment.getContainerHostById( hadoopClusterConfig.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting nameNode container host.", e );
            po.addLogFailed( "Error getting nameNode container host." );
            return;
        }

        executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigMasterCommand( namenode.getHostname(), hmasterContainerHost.getHostname() ),
                environment );


        // configure region servers
        po.addLog( "Configuring region servers..." );
        StringBuilder sb = new StringBuilder();
        for ( UUID uuid : config.getRegionServers() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host by id for region servers", e );
                po.addLogFailed( "Error getting container host by id for region servers" );
                return;
            }
        }
        executeCommandOnAllContainer( config.getAllNodes(), Commands.getConfigRegionCommand( sb.toString() ),
                environment );

        // configure quorum peers
        po.addLog( "Configuring quorum peers..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getQuorumPeers() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host for quorum peers", e );
                po.addLogFailed( "Error getting container host for quorum peers" );
                return;
            }
        }
        executeCommandOnAllContainer( config.getAllNodes(), Commands.getConfigQuorumCommand( sb.toString() ),
                environment );


        // configure back up master
        po.addLog( "Configuring backup masters..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getBackupMasters() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host for backup master", e );
                po.addLogFailed( "Error getting container host for backup master" );
                return;
            }
        }
        executeCommandOnAllContainer( config.getAllNodes(), Commands.getConfigBackupMastersCommand( sb.toString() ),
                environment );


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hBase.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "HBase cluster data saved into database" );

        try
        {
            hBase.subscribeToAlerts( environment );
        }
        catch ( MonitorException e )
        {
            LOG.error( "Failed to subscribe to alerts !", e );
            e.printStackTrace();
        }
    }


    public static void clearConfigurationFiles( Set<UUID> allUUIDs, Environment environment )
    {
        for ( UUID uuid : allUUIDs )
        {
            try
            {
                ContainerHost containerHost = environment.getContainerHostById( uuid );
                containerHost.execute( Commands.getRemoveRegionServerCommand() );
                containerHost.execute( Commands.getRemoveBackupMastersCommand() );
            }
            catch ( CommandException e )
            {
                LOG.error( "Error executing command on container", e );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host by uuid: " + uuid.toString(), e );
            }
        }
    }


    public static void executeCommandOnAllContainer( Set<UUID> allUUIDs, RequestBuilder command,
                                                     Environment environment )
    {
        for ( UUID uuid : allUUIDs )
        {
            try
            {
                ContainerHost containerHost = environment.getContainerHostById( uuid );
                containerHost.execute( command );
            }
            catch ( CommandException e )
            {
                LOG.error( "Error executing command on container", e );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host by uuid: " + uuid.toString(), e );
            }
        }
    }


    public void configureNewRegionServerNode( ConfigBase configBase, Environment environment, ContainerHost host )
            throws ClusterConfigurationException
    {
        HBaseConfig config = ( HBaseConfig ) configBase;
        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );

        // clear configuration files
        clearConfigurationFiles( Sets.newHashSet( host.getId() ), environment );

        // configure master
        po.addLog( "Configuring hmaster... !" );
        UUID hmaster = config.getHbaseMaster();
        ContainerHost hmasterContainerHost;
        try
        {
            hmasterContainerHost = environment.getContainerHostById( hmaster );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting hmaster container host.", e );
            po.addLogFailed( "Error getting hmaster container host." );
            return;
        }
        ContainerHost namenode;
        try
        {
            namenode = environment.getContainerHostById( hadoopClusterConfig.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting nameNode container host.", e );
            po.addLogFailed( "Error getting nameNode container host." );
            return;
        }

        executeCommandOnAllContainer( Sets.newHashSet( host.getId() ),
                Commands.getConfigMasterCommand( namenode.getHostname(), hmasterContainerHost.getHostname() ),
                environment );


        // configure region servers
        po.addLog( "Configuring region servers..." );
        StringBuilder sb = new StringBuilder();
        for ( UUID uuid : config.getRegionServers() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host by id for region servers", e );
                po.addLogFailed( "Error getting container host by id for region servers" );
                return;
            }
        }
        executeCommandOnAllContainer( Sets.newHashSet( host.getId() ), Commands.getConfigRegionCommand( sb.toString() ),
                environment );

        // configure quorum peers
        po.addLog( "Configuring quorum peers..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getQuorumPeers() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host for quorum peers", e );
                po.addLogFailed( "Error getting container host for quorum peers" );
                return;
            }
        }
        executeCommandOnAllContainer( Sets.newHashSet( host.getId() ), Commands.getConfigQuorumCommand( sb.toString() ),
                environment );


        // configure back up master
        po.addLog( "Configuring backup masters..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getBackupMasters() )
        {
            ContainerHost tmp;
            try
            {
                tmp = environment.getContainerHostById( uuid );
                sb.append( tmp.getHostname() );
                sb.append( " " );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error getting container host for backup master", e );
                po.addLogFailed( "Error getting container host for backup master" );
                return;
            }
        }
        executeCommandOnAllContainer( Sets.newHashSet( host.getId() ),
                Commands.getConfigBackupMastersCommand( sb.toString() ), environment );


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hBase.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "HBase cluster data saved into database" );


        //subscribe to alerts
        try
        {
            hBase.subscribeToAlerts( environment );
        }
        catch ( MonitorException e )
        {
            throw new ClusterConfigurationException( e );
        }
    }
}
