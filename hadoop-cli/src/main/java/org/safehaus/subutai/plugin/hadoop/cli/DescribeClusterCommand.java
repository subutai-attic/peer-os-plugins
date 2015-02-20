package org.safehaus.subutai.plugin.hadoop.cli;


import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      hadoop:describe-cluster test \ {cluster name}
 */
@Command( scope = "hadoop", name = "describe-clusters", description = "Shows the details of Hadoop cluster" )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false,
            description = "The name of the Hadoop cluster" )
    String clusterName;

    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    @Override
    protected Object doExecute()
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        if ( hadoopClusterConfig != null )
        {
            try
            {
                Environment environment = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() );
                StringBuilder sb = new StringBuilder();
                sb.append( "Cluster name: " ).append( hadoopClusterConfig.getClusterName() ).append( "\n" );
                sb.append( "Domain name: " ).append( hadoopClusterConfig.getDomainName() ).append( "\n" );
                sb.append( "All nodes:" ).append( "\n" );
                for ( UUID uuid : hadoopClusterConfig.getAllNodes() )
                {
                    try
                    {
                        ContainerHost host = environment.getContainerHostById( uuid );
                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                }
                sb.append( "Slave nodes:" ).append( "\n" );
                for ( UUID uuid : hadoopClusterConfig.getAllSlaveNodes() )
                {
                    try
                    {
                        ContainerHost host = environment.getContainerHostById( uuid );
                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                }
                sb.append( "Data nodes:" ).append( "\n" );
                for ( UUID uuid : hadoopClusterConfig.getDataNodes() )
                {
                    try
                    {
                        ContainerHost host = environment.getContainerHostById( uuid );
                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        e.printStackTrace();
                    }

                }
                sb.append( "Task trackers:" ).append( "\n" );
                for ( UUID uuid : hadoopClusterConfig.getTaskTrackers() )
                {
                    try
                    {
                        ContainerHost host = environment.getContainerHostById( uuid );
                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                }
                try
                {
                    UUID jt = hadoopClusterConfig.getJobTracker();
                    UUID nn = hadoopClusterConfig.getNameNode();
                    UUID snn = hadoopClusterConfig.getSecondaryNameNode();

                    ContainerHost namenode = environment.getContainerHostById( nn );
                    ContainerHost secnamenode = environment.getContainerHostById( snn );
                    ContainerHost jobTracker = environment.getContainerHostById( jt );

                    sb.append( "NameNode" ).append( "\n" );
                    sb.append( "   Hostname:" ).append( namenode.getHostname() ).append( "\n" );
                    sb.append( "   IPs:" ).append( namenode.getIpByInterfaceName( "eth0" ) ).append( "\n" );

                    sb.append( "SecondaryNameNode" ).append( "\n" );
                    sb.append( "   Hostname:" ).append( secnamenode.getHostname() ).append( "\n" );
                    sb.append( "   IPs:" ).append( secnamenode.getIpByInterfaceName( "eth0" ) ).append( "\n" );

                    sb.append( "Job tracker" ).append( "\n" );
                    sb.append( "   Hostname:" ).append( jobTracker.getHostname() ).append( "\n" );
                    sb.append( "   IPs:" ).append( jobTracker.getIpByInterfaceName( "eth0" ) ).append( "\n" );
                    System.out.println( sb.toString() );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        else
        {
            System.out.println( "No clusters found..." );
        }

        return null;
    }
}
