package org.safehaus.subutai.plugin.storm.impl.alert;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.CommandType;
import org.safehaus.subutai.plugin.storm.impl.Commands;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.safehaus.subutai.plugin.storm.impl.StormService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class StormAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( StormAlertListener.class.getName() );

    public static final String STORM_ALERT_LISTENER = "STORM_ALERT_LISTENER";
    private StormImpl storm;
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public StormAlertListener( final StormImpl storm )
    {
        this.storm = storm;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find storm cluster by environment id
        List<StormClusterConfiguration> clusters = storm.getClusters();

        StormClusterConfiguration targetCluster = null;
        for ( StormClusterConfiguration cluster : clusters )
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
        Environment environment = storm.getEnvironmentManager().getEnvironmentByUUID( metric.getEnvironmentId() );
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

        //check if source host belongs to found storm cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Storm cluster", metric.getHost() ) );
            return;
        }


        boolean isMasterNode = targetCluster.getNimbus().equals( sourceHost.getId() );

        //figure out Storm process pid
        int stormPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( isMasterNode ? new RequestBuilder( Commands.make(
                    CommandType.STATUS, StormService.NIMBUS) ):new RequestBuilder(
                    Commands.make( CommandType.STATUS, StormService.SUPERVISOR ) ), sourceHost );
            stormPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining Storm process PID", e );
        }

        //get Storm process resource usage by Storm pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( stormPID );

        //confirm that Storm is causing the stress, otherwise no-op
        MonitoringSettings thresholds = storm.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.9;
        boolean isCpuStressedByStorm = false;
        boolean isRamStressedByStorm = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressedByStorm = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressedByStorm = true;
        }

        if ( !( isRamStressedByStorm || isCpuStressedByStorm ) )
        {
            LOG.info( "Storm cluster ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedByStorm )
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
            else if ( isCpuStressedByStorm )
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
            storm.addNode( targetCluster.getClusterName() );
        }
        else
        {
            notifyUser();
        }
    }


    protected String parseService( String output, String target ) throws AlertException
    {
        Matcher m = Pattern.compile("(?m)^.*$").matcher( output );
        if ( m.find() ){
            if ( m.group().toLowerCase().contains( target.toLowerCase() ) ){
                return m.group();
            }
        }
        else{
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return null;
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
        return STORM_ALERT_LISTENER;
    }
}

