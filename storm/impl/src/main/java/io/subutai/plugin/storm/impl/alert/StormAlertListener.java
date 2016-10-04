package io.subutai.plugin.storm.impl.alert;


import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ExceededQuota;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.hub.share.resource.NumericValueResource;
import io.subutai.hub.share.resource.ResourceValue;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.CommandType;
import io.subutai.plugin.storm.impl.Commands;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.storm.impl.StormService;


/**
 * Node resource threshold excess alert listener
 */
public class StormAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( StormAlertListener.class.getName() );
    private static final String HANDLER_ID = "DEFAULT_PRESTO_EXCEEDED_QUOTA_ALERT_HANDLER";
    public static final String STORM_ALERT_LISTENER = "STORM_ALERT_LISTENER";
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;
    private StormImpl storm;
    private CommandUtil commandUtil = new CommandUtil();


    public StormAlertListener( final StormImpl storm )
    {
        this.storm = storm;
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find storm cluster by environment id
        List<StormClusterConfiguration> clusters = storm.getClusters();

        StormClusterConfiguration targetCluster = null;
        for ( StormClusterConfiguration cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environment.getId() ), null );
        }

        //        //get cluster environment
        //        Environment environment = storm.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
        //        if ( environment == null )
        //        {
        //            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId()
        // ), null );
        //        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( quotaAlertValue.getValue() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment",
                    quotaAlertValue.getValue().getHostId() ), null );
        }

        //check if source host belongs to found storm cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Storm cluster",
                    quotaAlertValue.getValue().getHostId() ) );
            return;
        }


        boolean isMasterNode = targetCluster.getNimbus().equals( sourceHost.getId() );

        //figure out Storm process pid
        int stormPID = 0;
        try
        {
            CommandResult result = commandUtil.execute(
                    isMasterNode ? new RequestBuilder( Commands.make( CommandType.STATUS, StormService.NIMBUS ) ) :
                    new RequestBuilder( Commands.make( CommandType.STATUS, StormService.SUPERVISOR ) ), sourceHost );
            stormPID = parsePid( result.getStdOut() );
        }
        catch ( Exception e )
        {
            throwAlertException( "Error obtaining Storm process PID", e );
        }

        //get Storm process resource usage by Storm pid
        ProcessResourceUsage processResourceUsage = null;
        try
        {
            processResourceUsage = storm.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), stormPID );

            //confirm that Storm is causing the stress, otherwise no-op
            MonitoringSettings thresholds = storm.getAlertSettings();
            ExceededQuota exceededQuota = quotaAlertValue.getValue();
            BigDecimal currentValue = exceededQuota.getCurrentValue( NumericValueResource.class ).getValue();

            double ramLimit = currentValue.doubleValue() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
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
        }
        catch ( MonitorException e )
        {
            e.printStackTrace();
        }
    }


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error( context, e );
        throw new AlertHandlerException( context, e );
    }


    protected int parsePid( String output ) throws Exception
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
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource exceeded threshold alert handler for storm cluster.";
    }
}

