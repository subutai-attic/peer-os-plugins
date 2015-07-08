package io.subutai.plugin.hadoop.cli;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 * hadoop:install-cluster test \ {cluster name}
 *                        intra.lan \ {domain name}
 *                        113c4b90-a59f-4001-be25-1d6eba2051fe \ {environment id}
 *                        2 \ {replication factor}
 *                        4c882c18-a312-4c8f-ac03-6ab3aeb1cab9 \ {namenode id}
 *                        4c882c18-a312-4c8f-ac03-6ab3aeb1cab9 \ {jobtracker id}
 *                        4c882c18-a312-4c8f-ac03-6ab3aeb1cab9 \ {secondary namenode id}
 *                        [ea28700c-3259-41ed-a85a-5cff3181b61e] \ {slave ids}
 */
@Command( scope = "hadoop", name = "install-cluster", description = "Command to install Hadoop cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;

    @Argument(index = 1, name = "domainName", description = "The domain name of the cluster.", required = true,
            multiValued = false)
    String domainName = null;

    @Argument(index = 2, name = "environmentId", description = "The environment id to be used to setup hadoop cluster", required = true,
            multiValued = false)
    String environmentId;

    @Argument(index = 3, name = "relicationFactor", description = "The replication factor for slave nodes", required = true,
            multiValued = false)
    int replicationFactor;

    @Argument( index = 4, name = "namenodeUUID", description = "The uuid of namenode container", required = true,
            multiValued = false )
    String namenodeUUID;

    @Argument( index = 5, name = "jobTrackerUUID", description = "The uuid of jobtracker container", required = true,
            multiValued = false )
    String jobTrackerUUID;

    @Argument( index = 6, name = "secondaryNamenodeUUID", description = "The uuid of secondarynamenode container", required = true,
            multiValued = false )
    String secondaryNamenodeUUID;


    @Argument( index = 7, name = "slaveNodes", description = "The uuid set of slaves nodes", required = true,
            multiValued = false )
    String slaveNodes[];

    private Hadoop hadoopManager;
    private Tracker tracker;

    @Override
    protected Object doExecute()
    {
        HadoopClusterConfig config = new HadoopClusterConfig();
        config.setEnvironmentId( UUID.fromString( environmentId  ));
        config.setClusterName( clusterName );
        config.setDomainName( domainName );
        config.setReplicationFactor( replicationFactor );
        config.setNameNode( UUID.fromString( namenodeUUID  ) );
        config.setJobTracker( UUID.fromString( jobTrackerUUID ) );
        config.setSecondaryNameNode( UUID.fromString( secondaryNamenodeUUID ) );
        List<UUID> slaves = new ArrayList<>();
        for ( String slave : slaveNodes ){
            slaves.add( UUID.fromString( slave ) );
        }
        config.setDataNodes( slaves ) ;
        config.setTaskTrackers( slaves );
        System.out.println( "Configuring " + clusterName + " hadoop cluster...");
        UUID uuid = hadoopManager.installCluster( config );
        System.out.println( "Install cluster operation is " +
                StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }

    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }

    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }
}
