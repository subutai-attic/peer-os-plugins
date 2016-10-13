package io.subutai.plugin.hadoop.cli;


import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


/**
 * sample command : hadoop:describe-cluster test \ {cluster name}
 */
@Command( scope = "hadoop", name = "describe-clusters", description = "Shows the details of Hadoop cluster" )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false,
            description = "The name of the Hadoop cluster" )
    String clusterName;

    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    @Override
    protected Object doExecute()
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        if ( hadoopClusterConfig != null )
        {
            try
            {
                Environment environment = environmentManager.loadEnvironment( hadoopClusterConfig.getEnvironmentId() );
                StringBuilder sb = new StringBuilder();
                sb.append( "Cluster name: " ).append( hadoopClusterConfig.getClusterName() ).append( "\n" );
                sb.append( "Domain name: " ).append( hadoopClusterConfig.getDomainName() ).append( "\n" );
                sb.append( "All nodes:" ).append( "\n" );
                for ( String id : hadoopClusterConfig.getAllNodes() )
                {
                    try
                    {
                        EnvironmentContainerHost host = environment.getContainerHostById( id );
                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                }
                sb.append( "Slave nodes:" ).append( "\n" );
//                for ( String id : hadoopClusterConfig.getAllSlaveNodes() )
//                {
//                    try
//                    {
//                        EnvironmentContainerHost host = environment.getContainerHostById( id );
//                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
//                    }
//                    catch ( ContainerHostNotFoundException e )
//                    {
//                        e.printStackTrace();
//                    }
//                }
                sb.append( "Data nodes:" ).append( "\n" );
//                for ( String id : hadoopClusterConfig.getDataNodes() )
//                {
//                    try
//                    {
//                        EnvironmentContainerHost host = environment.getContainerHostById( id );
//                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
//                    }
//                    catch ( ContainerHostNotFoundException e )
//                    {
//                        e.printStackTrace();
//                    }
//                }
                sb.append( "Task trackers:" ).append( "\n" );
//                for ( String id : hadoopClusterConfig.getTaskTrackers() )
//                {
//                    try
//                    {
//                        EnvironmentContainerHost host = environment.getContainerHostById( id );
//                        sb.append( "   Hostname: " ).append( host.getHostname() ).append( "\n" );
//                    }
//                    catch ( ContainerHostNotFoundException e )
//                    {
//                        e.printStackTrace();
//                    }
//                }
                //                    String jt = hadoopClusterConfig.getJobTracker();
                //                    String nn = hadoopClusterConfig.getNameNode();
                //                    String snn = hadoopClusterConfig.getSecondaryNameNode();

                //                    EnvironmentContainerHost namenode = environment.getContainerHostById( nn );
                //                    EnvironmentContainerHost secnamenode = environment.getContainerHostById( snn );
                //                    EnvironmentContainerHost jobTracker = environment.getContainerHostById( jt );

                //                    sb.append( "NameNode" ).append( "\n" );
                //                    sb.append( "   Hostname:" ).append( namenode.getHostname() ).append( "\n" );
                //                    sb.append( "   IPs:" ).append( namenode.getInterfaceByName( "eth0" ) ).append( "\n" );
                //
                //                    sb.append( "SecondaryNameNode" ).append( "\n" );
                //                    sb.append( "   Hostname:" ).append( secnamenode.getHostname() ).append( "\n" );
                //                    sb.append( "   IPs:" ).append( secnamenode.getInterfaceByName( "eth0" ) ).append( "\n" );
                //
                //                    sb.append( "Job tracker" ).append( "\n" );
                //                    sb.append( "   Hostname:" ).append( jobTracker.getHostname() ).append( "\n" );
                //                    sb.append( "   IPs:" ).append( jobTracker.getInterfaceByName( "eth0" ) ).append( "\n" );
                //                    System.out.println( sb.toString() );
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
}
