package org.safehaus.subutai.plugin.shark.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.plugin.shark.api.SharkClusterConfig;
import org.safehaus.subutai.plugin.shark.impl.SharkImpl;
import org.safehaus.subutai.plugin.spark.api.SparkClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class SharkAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( SharkAlertListener.class.getName() );

    private SharkImpl shark;
    public static final String SHARK_ALERT_LISTENER = "SHARK_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static final int MAX_RAM_QUOTA_MB = 2048;
    private static final int RAM_QUOTA_INCREMENT_MB = 512;
    private static final int MAX_CPU_QUOTA_PERCENT = 80;
    private static final int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public SharkAlertListener( final SharkImpl shark )
    {
        this.shark = shark;
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {

        //find shark cluster by environment id
        List<SharkClusterConfig> clusters = shark.getClusters();

        SharkClusterConfig targetCluster = null;
        for ( SharkClusterConfig cluster : clusters )
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
        Environment environment = shark.getEnvironmentManager().findEnvironment( metric.getEnvironmentId() );
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

        //check if source host belongs to found shark cluster
        if ( !targetCluster.getNodeIds().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Shark cluster", metric.getHost() ) );
            return;
        }

        //figure out Shark process pid
        int sharkPID;
        try
        {
            CommandResult result = commandUtil.execute( shark.getCommands().getServiceStatusCommand(), sourceHost );
            sharkPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new AlertException( "Error obtaining Shark process PID", e );
        }

        //get Shark process resource usage by Spark pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( sharkPID );

        //confirm that Shark is causing the stress, otherwise no-op
        MonitoringSettings thresholds = shark.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.9;
        boolean isCpuStressedByShark = false;
        boolean isRamStressedByShark = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressedByShark = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressedByShark = true;
        }

        if ( !( isRamStressedByShark || isCpuStressedByShark ) )
        {
            LOG.info( "Shark cluster ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedByShark )
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
            SparkClusterConfig sparkClusterConfig =
                    shark.getSparkManager().getCluster( targetCluster.getSparkClusterName() );
            if ( sparkClusterConfig == null )
            {
                throw new AlertException(
                        String.format( "Spark cluster %s not found", targetCluster.getSparkClusterName() ), null );
            }

            List<UUID> availableNodes = sparkClusterConfig.getAllNodesIds();
            availableNodes.removeAll( targetCluster.getNodeIds() );

            //no available nodes -> notify user
            if ( availableNodes.isEmpty() )
            {
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
                            String.format( "Could not obtain available spark node from environment by id %s",
                                    newNodeId ), null );
                }

                //launch node addition process
                shark.addNode( targetCluster.getClusterName(), newNodeHostName );
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
        return SHARK_ALERT_LISTENER;
    }
}
