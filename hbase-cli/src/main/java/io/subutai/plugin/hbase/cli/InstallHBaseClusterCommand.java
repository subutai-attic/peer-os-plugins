package io.subutai.plugin.hbase.cli;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;


@Command( scope = "hbase", name = "install-cluster", description = "Command to install HBase cluster" )
public class InstallHBaseClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "hbase cluster "
            + "name" )
    String clusterName;
    @Argument( index = 1, name = "hadoopClusterName", description = "The hostname list of worker nodes", required =
            true,
            multiValued = false )
    String hadoopClusterName;
    @Argument( index = 2, name = "hmaster", description = "The hostname of HMaster node", required = true,
            multiValued = false )
    String hmaster;
    @Argument( index = 3, name = "regionServers", description = "The hostname list of region server nodes", required
            = true,
            multiValued = false )
    String regionServers[] = null;
    @Argument( index = 4, name = "quorumPeers", description = "The hostname list of quorum peer nodes", required = true,
            multiValued = false )
    String quorumPeers[] = null;
    @Argument( index = 5, name = "backupMasters", description = "The hostname list of backup master nodes", required
            = true,
            multiValued = false )
    String backupMasters[] = null;
    private Tracker tracker;
    private HBase hbaseManager;
    private Hadoop hadoopManager;


    protected Object doExecute()
    {
        HBaseConfig config = new HBaseConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );
        config.setHbaseMaster( hmaster );

        Set<String> regionList = new HashSet<>();
        Collections.addAll( regionList, regionServers );
        config.setRegionServers( regionList );

        Set<String> quorumList = new HashSet<>();
        Collections.addAll( quorumList, quorumPeers );
        config.setQuorumPeers( quorumList );

        Set<String> backupMasterList = new HashSet<>();
        Collections.addAll( backupMasterList, backupMasters );
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


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
