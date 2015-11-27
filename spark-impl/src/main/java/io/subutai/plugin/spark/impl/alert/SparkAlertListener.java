package io.subutai.plugin.spark.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.spark.api.SparkClusterConfig;
import io.subutai.plugin.spark.impl.SparkImpl;


/**
 * Node resource threshold excess alert listener
 */
public class SparkAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( SparkAlertListener.class.getName() );

    public static final String SPARK_ALERT_LISTENER = "SPARK_ALERT_LISTENER";
    private SparkImpl spark;
    private CommandUtil commandUtil = new CommandUtil();
    private static double MAX_RAM_QUOTA_MB = 3072;
    private static final int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static final int MAX_CPU_QUOTA_PERCENT = 100;
    private static final int CPU_QUOTA_INCREMENT_PERCENT = 15;


    public SparkAlertListener( final SparkImpl spark )
    {
        this.spark = spark;
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
            throw new AlertException(
                    String.format( "Cluster not found by environment id %s", metric.getEnvironmentId() ), null );
        }

        //get cluster environment
        Environment environment = spark.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new AlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ),
                    null );
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( metric.getHostId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new AlertException(
                    String.format( "Alert source host %s not found in environment", metric.getHost() ), null );
        }

        //check if source host belongs to found spark cluster
        if ( !targetCluster.getAllNodesIds().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Spark cluster", metric.getHost() ) );
            return;
        }

        boolean isMasterNode = targetCluster.getMasterNodeId().equals( sourceHost.getId() );

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        //        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.8;

        //figure out Spark process pid
        int sparkPID;
        try
        {
            CommandResult result = commandUtil.execute( isMasterNode ? spark.getCommands().getObtainMasterPidCommand() :
                                                        spark.getCommands().getObtainSlavePidCommand(), sourceHost );
            sparkPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new AlertException( "Error obtaining Spark process PID", e );
        }

        //get Spark process resource usage by Spark pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( sparkPID );

        //confirm that Spark is causing the stress, otherwise no-op
        MonitoringSettings thresholds = spark.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.7;
        boolean isCpuStressedBySpark = false;
        boolean isRamStressedBySpark = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressedBySpark = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressedBySpark = true;
        }

        if ( !( isRamStressedBySpark || isCpuStressedBySpark ) )
        {
            LOG.info( "Spark cluster ok" );
            return;
        }

        //auto-scaling is enabled -> scale cluster
        //        if ( targetCluster.isAutoScaling() )
        //        {
        // check if a quota limit increase does it
        //            boolean quotaIncreased = false;

        //            if ( isRamStressedBySpark )
        //            {
        //                //read current RAM quota
        //                int ramQuota = sourceHost.getRamQuota();
        //
        //                if ( ramQuota < MAX_RAM_QUOTA_MB )
        //                {
        //
        //                    // if available quota on resource host is greater than 10 % of calculated increase
        // amount,
        //                    // increase quota, otherwise scale horizontally
        //                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
        //                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
        //                    {
        //
        //                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost
        // .getHostname(),
        //                                sourceHost.getRamQuota(), newRamQuota );
        //                        //we can increase RAM quota
        //                        sourceHost.setRamQuota( newRamQuota );
        //
        //                        quotaIncreased = true;
        //                    }
        //                }
        //            }

        //            if ( isCpuStressedBySpark )
        //            {
        //                //read current CPU quota
        //                int cpuQuota = sourceHost.getCpuQuota();
        //                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
        //                {
        //                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota +
        // CPU_QUOTA_INCREMENT_PERCENT );
        //                    LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
        // cpuQuota,
        //                            newCpuQuota );
        //                    //we can increase CPU quota
        //                    sourceHost.setCpuQuota( newCpuQuota );
        //
        //                    quotaIncreased = true;
        //                }
        //            }
        //
        //            //quota increase is made, return
        //            if ( quotaIncreased )
        //            {
        //                return;
        //            }

        // add new node
        //            LOG.info( "Adding new node to {} spark cluster", targetCluster.getClusterName() );
        //            HadoopClusterConfig hadoopClusterConfig =
        //                    spark.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
        //            if ( hadoopClusterConfig == null )
        //            {
        //                throw new AlertException(
        //                        String.format( "Hadoop cluster %s not found", targetCluster
        // .getHadoopClusterName() ), null );
        //            }
        //
        //            List<String> availableNodes = hadoopClusterConfig.getAllNodes();
        //            availableNodes.removeAll( targetCluster.getAllNodesIds() );
        //
        //            //no available nodes or master node is stressed -> notify user
        //            if ( availableNodes.isEmpty() || isMasterNode )
        //            {
        //                //for master node we can use only vertical scaling, so we need to notify user
        //                notifyUser();
        //            }
        //            //add first available node
        //            else
        //            {
        //                String newNodeId = availableNodes.iterator().next();
        //                String newNodeHostName = null;
        //                for ( EnvironmentContainerHost containerHost : containers )
        //                {
        //                    if ( containerHost.getId().equals( newNodeId ) )
        //                    {
        //                        newNodeHostName = containerHost.getHostname();
        //                        break;
        //                    }
        //                }
        //
        //                if ( newNodeHostName == null )
        //                {
        //                    throw new AlertException(
        //                            String.format( "Could not obtain available hadoop node from environment by
        // id %s",
        //                                    newNodeId ), null );
        //                }
        //
        //                //launch node addition process
        //                spark.addSlaveNode( targetCluster.getClusterName(), newNodeHostName );
        //            }
        //        }
        //        else
        //        {
        //            notifyUser();
        //        }
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
            throw new AlertException( String.format( "Could not parse PID from %s", output ), null );
        }
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

