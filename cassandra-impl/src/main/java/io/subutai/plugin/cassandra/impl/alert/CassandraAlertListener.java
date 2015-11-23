package io.subutai.plugin.cassandra.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.quota.RamQuota;
import io.subutai.common.quota.RamQuotaUnit;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.Commands;


/**
 * Node resource threshold excess alert listener
 */
public class CassandraAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( CassandraAlertListener.class.getName() );
    public static final String CASSANDRA_ALERT_LISTENER = "CASSANDRA_ALERT_LISTENER";
    private CassandraImpl cassandra;
    private CommandUtil commandUtil = new CommandUtil();
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;


    public CassandraAlertListener( final CassandraImpl cassandra )
    {
        this.cassandra = cassandra;
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
        List<CassandraClusterConfig> clusters = cassandra.getClusters();

        CassandraClusterConfig targetCluster = null;
        for ( CassandraClusterConfig cluster : clusters )
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
        Environment environment = null;
        try
        {
            environment = cassandra.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {

            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
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
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
                    null );
        }

        //check if source host belongs to found cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Cassandra cluster", metric.getHost() ) );
            return;
        }

        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota().getRamQuotaValue( RamQuotaUnit.MB ) * 0.5;


        //figure out process pid
        int processPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( new RequestBuilder( Commands.statusCommand ), sourceHost );
            processPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }

        //get process resource usage by pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( processPID );

        //confirm that Cassandra is causing the stress, otherwise no-op
        MonitoringSettings thresholds = cassandra.getAlertSettings();
        double ramLimit = metric.getTotalRam() * thresholds.getRamAlertThreshold() / 100; // 0.8
        double redLine = 0.7;
        boolean isCpuStressed = false;
        boolean isRamStressed = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressed = true;
        }

        if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressed = true;
        }

        if ( !( isRamStressed || isCpuStressed ) )
        {
            LOG.info( "Cassandra cluster is not stressed, returning." );
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
                double ramQuota = sourceHost.getRamQuota().getRamQuotaValue( RamQuotaUnit.MB );

                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {

                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
                    {

                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                sourceHost.getRamQuota(), newRamQuota );
                        //we can increase RAM quota
                        RamQuota quota = new RamQuota( RamQuotaUnit.MB, ( long ) newRamQuota );
                        sourceHost.setRamQuota( quota );

                        quotaIncreased = true;
                    }
                }
            }

            if ( isCpuStressed )
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota().getPercentage();
                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
                    LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(), cpuQuota,
                            newCpuQuota );
                    //we can increase CPU quota
                    sourceHost.setCpuQuota( newCpuQuota );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            //launch node addition process
            LOG.info( "Adding new node to {} cassandra cluster", targetCluster.getClusterName() );
            cassandra.addNode( targetCluster.getClusterName() );
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
        return CASSANDRA_ALERT_LISTENER;
    }
}
