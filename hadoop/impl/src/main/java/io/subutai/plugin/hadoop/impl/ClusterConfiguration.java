package io.subutai.plugin.hadoop.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );
    private TrackerOperation po;
    private HadoopImpl hadoopManager;


    public ClusterConfiguration( final TrackerOperation operation, final HadoopImpl hadoop )
    {
        this.po = operation;
        this.hadoopManager = hadoop;
    }


    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        HadoopClusterConfig config = ( HadoopClusterConfig ) configBase;

        EnvironmentContainerHost namenode;
        try
        {
            po.addLog( String.format( "Configuring cluster: %s", configBase.getClusterName() ) );

            namenode = environment.getContainerHostById( config.getNameNode() );

            // create directory for namenode
            executeCommandOnContainer( namenode, Commands.getCreateNamenodeDirectoryCommand() );

            // update hdfs-site.xml
            executeCommandOnContainer( namenode, Commands.getUpdateHdfsMaster() );

            // update core-site.xml
            executeCommandOnContainer( namenode, Commands.getUpdateCore() );

            // update yarn-site.xml
            executeCommandOnContainer( namenode, Commands.getUpdateYarn() );

            // create mapred-site.xml
            executeCommandOnContainer( namenode, Commands.getCreateMapred() );

            // configure hdfs-site.xml
            executeCommandOnContainer( namenode, Commands.getSetNamenodeIp(
                    namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );

            // configure core-site.xml
            executeCommandOnContainer( namenode, Commands.getSetNamenodeIpCore(
                    namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );

            // set replication
            executeCommandOnContainer( namenode, Commands.getSetReplication( config.getReplicationFactor() ) );

            // collecting slaves ip
            Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );

            String slaveIPs = collectSlavesIp( slaves );

            // set slaves
            executeCommandOnContainer( namenode, Commands.getSetSlavesCommand( slaveIPs ) );

            // Configure slaves
            for ( final EnvironmentContainerHost slave : slaves )
            {
                configureSlave( namenode, slave, config );
            }

            // format hdfs
            executeCommandOnContainer( namenode, Commands.getFormatHdfs() );

            // start cluster
            executeCommandOnContainer( namenode, Commands.getStartDfsCommand() );
            executeCommandOnContainer( namenode, Commands.getStartYarnCommand() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting container host for name node.", e );
            po.addLogFailed( "Error getting container host for name node." );
            throw new ClusterConfigurationException( e );
        }


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hadoopManager.getPluginDAO()
                     .saveInfo( HadoopClusterConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "Hadoop cluster data saved into database" );
    }


    private String collectSlavesIp( final Set<EnvironmentContainerHost> slaves )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost slave : slaves )
        {
            sb.append( slave.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ).append( "\n" );
        }

        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }

        return sb.toString();
    }


    private void configureSlave( final EnvironmentContainerHost namenode, final EnvironmentContainerHost slave,
                                 final HadoopClusterConfig config )
    {
        // creating data node directory
        executeCommandOnContainer( slave, Commands.getCreateDatanodeDirectoryCommand() );

        // update hdfs-site.xml
        executeCommandOnContainer( slave, Commands.getUpdateHdfsSlave() );

        // update core-site.xml
        executeCommandOnContainer( slave, Commands.getUpdateCore() );

        // update yarn-site.xml
        executeCommandOnContainer( slave, Commands.getUpdateYarn() );

        // create mapred-site.xml
        executeCommandOnContainer( slave, Commands.getCreateMapred() );

        // configure core-site.xml
        executeCommandOnContainer( slave, Commands.getSetNamenodeIpCore(
                namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );

        // set replication
        executeCommandOnContainer( slave, Commands.getSetReplication( config.getReplicationFactor() ) );

        // configure hdfs-site.xml
        executeCommandOnContainer( slave, Commands.getSetNamenodeIp(
                namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );
    }


    private void executeCommandOnContainer( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            containerHost.execute( new RequestBuilder( command ).withTimeout( 20 ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error while executing \"" + command + "\"." );
            e.printStackTrace();
        }
    }


    public void deleteClusterConfiguration( final HadoopClusterConfig config, final Environment environment )
            throws ContainerHostNotFoundException
    {
        // clean up datanode configuration
        EnvironmentContainerHost datanode = environment.getContainerHostById( config.getNameNode() );

        executeCommandOnContainer( datanode, Commands.getStopDfsCommand() );
        executeCommandOnContainer( datanode, Commands.getStopYarnCommand() );

        executeCommandOnContainer( datanode, Commands.getCleanHdfs() );
        executeCommandOnContainer( datanode, Commands.getCleanCore() );
        executeCommandOnContainer( datanode, Commands.getCleanYarn() );
        executeCommandOnContainer( datanode, Commands.getCleanMapred() );
        executeCommandOnContainer( datanode, Commands.getSetSlavesCommand( "localhost" ) );
        executeCommandOnContainer( datanode, Commands.getDeleteNamenodeFolder() );

        Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );

        for ( final EnvironmentContainerHost node : slaves )
        {
            executeCommandOnContainer( node, Commands.getCleanHdfs() );
            executeCommandOnContainer( node, Commands.getCleanCore() );
            executeCommandOnContainer( node, Commands.getCleanYarn() );
            executeCommandOnContainer( node, Commands.getCleanMapred() );
            executeCommandOnContainer( node, Commands.getDeleteDataNodeFolder() );
        }
    }


    public void addNode( final Environment environment, final HadoopClusterConfig config,
                         final EnvironmentContainerHost newNode ) throws ContainerHostNotFoundException
    {
        EnvironmentContainerHost namenode = environment.getContainerHostById( config.getNameNode() );

        if ( config.getExcludedSlaves().isEmpty() )
        {

            // Stop cluster
            executeCommandOnContainer( namenode, Commands.getStopDfsCommand() );
            executeCommandOnContainer( namenode, Commands.getStopYarnCommand() );

            Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );
            String slaveIps = collectSlavesIp( slaves );
            int repl = slaves.size();
            config.setReplicationFactor( String.valueOf( repl ) );

            // configure new slave node
            configureSlave( namenode, newNode, config );

            // update configuration of namenode
            executeCommandOnContainer( namenode, Commands.getSetSlavesCommand( slaveIps ) );
            executeCommandOnContainer( namenode, Commands.getUpdateHdfsMaster() );
            executeCommandOnContainer( namenode, Commands.getSetNamenodeIp(
                    namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );
            executeCommandOnContainer( namenode, Commands.getSetReplication( config.getReplicationFactor() ) );

            // update configuration on slaves
            for ( final EnvironmentContainerHost slave : slaves )
            {
                executeCommandOnContainer( slave, Commands.getUpdateHdfsSlave() );
                executeCommandOnContainer( namenode, Commands.getSetNamenodeIp(
                        namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );
                executeCommandOnContainer( namenode, Commands.getSetReplication( config.getReplicationFactor() ) );
            }

            executeCommandOnContainer( namenode, Commands.getStartDfsCommand() );
            executeCommandOnContainer( namenode, Commands.getStartYarnCommand() );
        }
        else
        {
            // adding decommissed node
            executeCommandOnContainer( namenode,
                    Commands.getIncludeCommand( config.getExcludedSlaves().iterator().next() ) );
            executeCommandOnContainer( namenode, Commands.getCommentExcludeSettingsCommand() );
            executeCommandOnContainer( namenode, Commands.getRefreshNodesCommand() );
            config.getExcludedSlaves()
                  .remove( newNode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() );
        }
    }


    public void excludeNode( final EnvironmentContainerHost namenode, final EnvironmentContainerHost slave,
                             final HadoopClusterConfig config, final Environment environment )
    {
        // add slave ip to dfs.exclude
        executeCommandOnContainer( namenode,
                Commands.getExcludeCommand( slave.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ) );

        // uncomment exclude in hdfs-site.xml
        executeCommandOnContainer( namenode, Commands.getUncommentExcludeSettingsCommand() );

        // refresh nodes
        executeCommandOnContainer( namenode, Commands.getRefreshNodesCommand() );
    }
}
