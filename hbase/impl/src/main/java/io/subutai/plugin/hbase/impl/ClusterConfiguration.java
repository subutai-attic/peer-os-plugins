package io.subutai.plugin.hbase.impl;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.host.HostId;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hbase.api.HBaseConfig;


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

        po.addLog( "Configuring Hbase cluster... !" );
        EnvironmentContainerHost hmasterContainerHost;
        try
        {
            hmasterContainerHost = environment.getContainerHostById( config.getHbaseMaster() );
            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );

            // configure all nodes
            for ( final EnvironmentContainerHost node : allNodes )
            {
                configureNode( hmasterContainerHost, node );
            }

            // collecting slaves ip
            Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getRegionServers() );

            String slavesHostnames = collectSlavesHostnames( slaves );


            // Configure hbasemaster
            executeCommand( hmasterContainerHost, Commands.setRegionServers( slavesHostnames ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
            LOG.error( "Error getting hmaster container host.", e );
            po.addLogFailed( "Error getting hmaster container host." );
            return;
        }

        po.addLog( "Configuring reverse proxy for web console" );
        configureReverseProxy( hmasterContainerHost, hmasterContainerHost.getHostname().toLowerCase() + ".hbase" );

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


    private void configureNode( final ContainerHost hmasterContainerHost, final EnvironmentContainerHost node )
    {
        String hmasterIp = hmasterContainerHost.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp();

        // set Hbase master info port
        executeCommand( node, Commands.setHbaseMasterInfoPort() );

        // set Hbase rootdir
        executeCommand( node, Commands.setHbaseRootDir( hmasterIp ) );

        // set Hbase cluster distributed value
        executeCommand( node, Commands.setHbaseClusterDistributed() );

        // set Hbase Zookeeper quorum
        executeCommand( node, Commands.setHbaseZookeeperQuorum( hmasterIp ) );

        // set Hbase Zookeeper DataDir
        executeCommand( node, Commands.setHbaseZookeeperDataDir( hmasterIp ) );

        // set Hbase Zookeeper clientPort
        executeCommand( node, Commands.setHbaseZookeeperClientPort() );
    }


    public void clearConfigurationFiles( final HBaseConfig config, final EnvironmentContainerHost node,
                                         final Environment environment )
    {
        try
        {
            EnvironmentContainerHost master = environment.getContainerHostById( config.getHbaseMaster() );
            master.execute( Commands.getRemoveRegionServerCommand( node.getHostname() ) );
            node.execute( Commands.getStopRegionServerCommand() );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error executing command on container", e );
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting container host by uuid: " + config.getHbaseMaster(), e );
            e.printStackTrace();
        }
    }


    private void executeCommand( ContainerHost host, String command )
    {
        try
        {
            host.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private String collectSlavesHostnames( final Set<EnvironmentContainerHost> slaves )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost slave : slaves )
        {
            sb.append( slave.getHostname() ).append( "\n" );
        }

        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }

        return sb.toString();
    }


    public void addnode( final HBaseConfig config, final EnvironmentContainerHost node, final Environment environment )
    {
        try
        {
            EnvironmentContainerHost master = environment.getContainerHostById( config.getHbaseMaster() );
            master.execute( Commands.getAddRegionServerCommand( node.getHostname() ) );
            configureNode( master, node );
            node.execute( Commands.getStartRegionServerCommand() );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error executing command on container", e );
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting container host by uuid: " + config.getHbaseMaster(), e );
            e.printStackTrace();
        }
    }


    private void configureReverseProxy( final EnvironmentContainerHost hbaseMaster, final String domainName )
    {
        PeerManager peerManager = hBase.getPeerManager();
        LocalPeer localPeer = peerManager.getLocalPeer();
        String ipAddress = hbaseMaster.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp();

        try
        {
            ResourceHost resourceHost = null;
            try
            {
                resourceHost = localPeer.getResourceHostByContainerId( hbaseMaster.getContainerId().toString() );
            }
            catch ( HostNotFoundException e )
            {
                LOG.error( e.toString() );
                HostId hid = null;
                try
                {
                    hid = peerManager.getPeer( peerManager.getRemotePeerIdByIp( ipAddress ) )
                                     .getResourceHostIdByContainerId( hbaseMaster.getContainerId() );
                    resourceHost = ( ResourceHost ) hid;
                }
                catch ( PeerException ex )
                {
                    LOG.error( ex.toString() );
                    po.addLogFailed( "NO HOST FOUND!!!" );
                }
            }
            finally
            {
                if ( resourceHost == null )
                {
                    LOG.error( "No ResourceHost connected" );
                    po.addLogFailed( "No ResourceHost connected" );
                }
            }

            LOG.info( "resouceHostID: " + resourceHost );

            String vlanString = UUID.randomUUID().toString();
            resourceHost.execute( new RequestBuilder( String.format( "subutai proxy del %s -d", vlanString ) ) );
            resourceHost.execute( new RequestBuilder(
                    String.format( "subutai proxy add %s -d %s -f /mnt/lib/lxc/%s/rootfs/etc/nginx/ssl.pem", vlanString,
                            domainName, hbaseMaster.getHostname() ) ) );
            resourceHost.execute(
                    new RequestBuilder( String.format( "subutai proxy add %s -h %s:16010", vlanString, ipAddress ) ) );
        }
        catch ( CommandException ex )
        {
            LOG.error( ex.toString() );
            po.addLogFailed( ex.toString() );
        }
    }
}
