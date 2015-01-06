package org.safehaus.subutai.plugin.spark.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.spark.api.SparkClusterConfig;
import org.safehaus.subutai.plugin.spark.impl.SparkImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class SparkAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( SparkAlertListener.class.getName() );

    public static final String SPARK_ALERT_LISTENER = "SPARK_ALERT_LISTENER";
    private SparkImpl spark;
    private CommandUtil commandUtil = new CommandUtil();


    public SparkAlertListener( final SparkImpl spark )
    {
        this.spark = spark;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find spark cluster by environment id
        List<SparkClusterConfig> clusters = spark.getClusters();

        SparkClusterConfig targetCluster = null;
        for ( SparkClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( metric.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", metric.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment = spark.getEnvironmentManager().getEnvironmentByUUID( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert's source host
        Set<ContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getHostname().equalsIgnoreCase( metric.getHost() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
                    null );
        }

        //check if source host belongs to found spark cluster
        if ( !targetCluster.getAllNodesIds().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Spark cluster", metric.getHost() ) );
            return;
        }


        boolean isMasterNode = targetCluster.getMasterNodeId().equals( sourceHost.getId() );

        //figure out Spark process pid
        int sparkPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( isMasterNode ? spark.getCommands().getObtainMasterPidCommand() :
                                                        spark.getCommands().getObtainSlavePidCommand(), sourceHost );
            sparkPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining Spark process PID", e );
        }

        //get Spark process resource usage by Spark pid
        ProcessResourceUsage processResourceUsage = spark.getMonitor().getProcessResourceUsage( sourceHost, sparkPID );

        //confirm that Spark is causing the stress, otherwise no-op
        MonitoringSettings thresholds = spark.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.9;
        boolean cpuIsStressed = false;
        boolean ramIsStressed = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            ramIsStressed = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            cpuIsStressed = true;
        }

        if ( !( ramIsStressed || cpuIsStressed ) )
        {
            LOG.info( "Spark cluster ok" );
            return;
        }

        //if cluster has auto-scaling enabled:
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean canIncreaseQuota = false;

            //TODO implement checking if quota increase works

            //increase quota limit
            if ( canIncreaseQuota )
            {
                //TODO implement vertical scaling
                return;
            }


            // add new node
            HadoopClusterConfig hadoopClusterConfig =
                    spark.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throwAlertException(
                        String.format( "Hadoop cluster %s not found", targetCluster.getHadoopClusterName() ), null );
            }

            List<UUID> availableNodes = hadoopClusterConfig.getAllNodes();
            availableNodes.removeAll( targetCluster.getAllNodesIds() );

            //no available nodes or master node is stressed -> notify user
            if ( availableNodes.isEmpty() || isMasterNode )
            {
                //for master node we can use only vertical scaling, so we need to notify user
                notifyUser();
            }
            //add first available node
            else
            {
                UUID newNodeId = availableNodes.iterator().next();
                String newNodeHostName = null;
                for ( ContainerHost containerHost : containers )
                {
                    if ( containerHost.getId().equals( newNodeId ) )
                    {
                        newNodeHostName = containerHost.getHostname();
                        break;
                    }
                }

                if ( newNodeHostName == null )
                {
                    throwAlertException( "", null );
                }

                //launch node addition process
                spark.addSlaveNode( targetCluster.getClusterName(), newNodeHostName );
            }
        }
        else
        {
            notifyUser();
        }
    }


    protected int parsePid( String output ) throws AlertException
    {
        Pattern p = Pattern.compile( "pid\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE );

        Matcher m = p.matcher( output );

        if ( m.find() )
        {
            return Integer.parseInt( m.group( 1 ) );
        }
        else
        {
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return 0;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }


    @Override
    public String getSubscriberId()
    {
        return SPARK_ALERT_LISTENER;
    }
}

