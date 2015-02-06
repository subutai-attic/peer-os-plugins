package org.safehaus.subutai.plugin.spark.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
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
    private static final int MAX_RAM_QUOTA_MB = 2048;
    private static final int RAM_QUOTA_INCREMENT_MB = 512;
    private static final int MAX_CPU_QUOTA_PERCENT = 80;
    private static final int CPU_QUOTA_INCREMENT_PERCENT = 10;


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
        Environment environment = spark.getEnvironmentManager().findEnvironment( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new AlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ),
                    null );
        }

        //get environment containers and find alert's source host
        Set<ContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
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
        double redLine = 0.9;
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
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedBySpark )
            {
                //read current RAM quota
                int ramQuota = sourceHost.getRamQuota();


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    //we can increase RAM quota
                    sourceHost.setRamQuota( Math.min( MAX_RAM_QUOTA_MB, ramQuota + RAM_QUOTA_INCREMENT_MB ) );

                    quotaIncreased = true;
                }
            }
            else
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    //we can increase CPU quota
                    sourceHost.setCpuQuota( Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT ) );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            // add new node
            HadoopClusterConfig hadoopClusterConfig =
                    spark.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throw new AlertException(
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
                    throw new AlertException(
                            String.format( "Could not obtain available hadoop node from environment by id %s",
                                    newNodeId ), null );
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

