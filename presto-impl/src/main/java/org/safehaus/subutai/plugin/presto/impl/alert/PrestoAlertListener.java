package org.safehaus.subutai.plugin.presto.impl.alert;


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
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;
import org.safehaus.subutai.plugin.presto.impl.PrestoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class PrestoAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( PrestoAlertListener.class.getName() );

    public static final String PRESTO_ALERT_LISTENER = "PRESTO_ALERT_LISTENER";
    private PrestoImpl presto;
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public PrestoAlertListener( final PrestoImpl presto )
    {
        this.presto = presto;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find cluster by environment id
        List<PrestoClusterConfig> clusters = presto.getClusters();

        PrestoClusterConfig targetCluster = null;
        for ( PrestoClusterConfig cluster : clusters )
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
        Environment environment = presto.getEnvironmentManager().getEnvironmentByUUID( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
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
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
                    null );
        }

        //check if source host belongs to found cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Presto cluster", metric.getHost() ) );
            return;
        }

        //figure out process pid
        int prestoPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( presto.getCommands().getStatusCommand(), sourceHost );
            prestoPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }


        //get process resource usage by pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( prestoPID );

        //confirm that Presto is causing the stress, otherwise no-op
        MonitoringSettings thresholds = presto.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.9;
        boolean isCpuStressed = false;
        boolean isRamStressed = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressed = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressed = true;
        }

        if ( !( isRamStressed || isCpuStressed ) )
        {
            LOG.info( "Presto cluster ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressed )
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
            else if ( isCpuStressed )
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
                    presto.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throwAlertException(
                        String.format( "Hadoop cluster %s not found", targetCluster.getHadoopClusterName() ), null );
            }

            boolean isCoordinator = sourceHost.getId().equals( targetCluster.getCoordinatorNode() );
            List<UUID> availableNodes = hadoopClusterConfig.getAllNodes();
            availableNodes.removeAll( targetCluster.getAllNodes() );

            //no available nodes or coordinator node is stressed -> notify user
            if ( availableNodes.isEmpty() || isCoordinator )
            {
                //for coordinator node we can use only vertical scaling, so we need to notify user
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
                    throwAlertException(
                            String.format( "Could not obtain available hadoop node from environment by id %s",
                                    newNodeId ), null );
                }

                //launch node addition process
                presto.addWorkerNode( targetCluster.getClusterName(), newNodeHostName );
            }
        }
        else
        {
            notifyUser();
        }
    }


    protected int parsePid( String output ) throws AlertException
    {
        Pattern p = Pattern.compile( "(\\d+)", Pattern.CASE_INSENSITIVE );

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
        return PRESTO_ALERT_LISTENER;
    }
}
