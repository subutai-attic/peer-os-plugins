package org.safehaus.subutai.plugin.hbase.impl;


import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOG = Logger.getLogger( ClusterConfiguration.class.getName() );
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

        // configure master
        po.addLog( "Configuring hmaster... !" );
        UUID hmaster = config.getHbaseMaster();
        ContainerHost hmasterContainerHost = environment.getContainerHostById( hmaster );
        ContainerHost namenode = environment.getContainerHostById( hadoopClusterConfig.getNameNode() );

        executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigMasterCommand( namenode.getHostname(), hmasterContainerHost.getHostname() ), environment );


        // configure region servers
        po.addLog( "Configuring region servers..." );
        StringBuilder sb = new StringBuilder();
        for ( UUID uuid : config.getRegionServers() )
        {
            ContainerHost tmp = environment.getContainerHostById( uuid );
            sb.append( tmp.getHostname() );
            sb.append( " " );
        }
        executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigRegionCommand( sb.toString() ), environment );

        // configure quorum peers
        po.addLog( "Configuring quorum peers..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getQuorumPeers() )
        {
            ContainerHost tmp = environment.getContainerHostById( uuid );
            sb.append( tmp.getHostname() );
            sb.append( " " );
        }
        executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigQuorumCommand( sb.toString() ), environment );


        // configure back up master
        po.addLog( "Configuring backup masters..." );
        sb = new StringBuilder();
        for ( UUID uuid : config.getBackupMasters() )
        {
            ContainerHost tmp = environment.getContainerHostById( uuid );
            sb.append( tmp.getHostname() );
            sb.append( " " );
        }
        executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigBackupMastersCommand( sb.toString() ), environment );


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hBase.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "Hadoop cluster data saved into database" );
    }


    private void executeCommandOnAllContainer( Set<UUID> allUUIDs, RequestBuilder command, Environment environment ){
        for ( UUID uuid : allUUIDs ){
            try
            {
                ContainerHost containerHost = environment.getContainerHostById( uuid );
                containerHost.execute( command );
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }
    }
}
