package io.subutai.plugin.hbase.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "hbase", name = "install-cluster", description = "Command to install HBase cluster" )
public class InstallHBaseClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "hbase cluster "
            + "name" ) String clusterName;
    @Argument( index = 1, name = "hadoopClusterName", description = "The hostname list of worker nodes", required =
            true,
            multiValued = false ) String hadoopClusterName;
    @Argument( index = 2, name = "hmaster", description = "The hostname of HMaster node", required = true,
            multiValued = false ) String hmaster;
    @Argument( index = 3, name = "regionServers", description = "The hostname list of region server nodes", required
            = true,
            multiValued = false ) String regionServers[] = null;
    @Argument( index = 4, name = "quorumPeers", description = "The hostname list of quorum peer nodes", required = true,
            multiValued = false ) String quorumPeers[] = null;
    @Argument( index = 5, name = "backupMasters", description = "The hostname list of backup master nodes", required
            = true,
            multiValued = false ) String backupMasters[] = null;
    private Tracker tracker;
    private HBase hbaseManager;
    private Hadoop hadoopManager;


    protected Object doExecute()
    {
        HBaseConfig config = new HBaseConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );
        config.setHbaseMaster( UUID.fromString( hmaster ) );

        Set<UUID> regionList = new HashSet<>();
        for ( String s : regionServers )
        {
            regionList.add( UUID.fromString( s ) );
        }
        config.setRegionServers( regionList );

        Set<UUID> quorumList = new HashSet<>();
        for ( String s : quorumPeers )
        {
            quorumList.add( UUID.fromString( s ) );
        }
        config.setQuorumPeers( quorumList );

        Set<UUID> backupMasterList = new HashSet<>();
        for ( String s : backupMasters )
        {
            backupMasterList.add( UUID.fromString( s ) );
        }
        config.setBackupMasters( backupMasterList );

        UUID uuid = hbaseManager.installCluster( config );
        System.out.println(
                "Install operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public HBase getHbaseManager()
    {
        return hbaseManager;
    }


    public void setHbaseManager( HBase hbaseManager )
    {
        this.hbaseManager = hbaseManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
