package io.subutai.plugin.storm.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertResource;
import io.subutai.common.peer.AlertListener;
import io.subutai.common.peer.AlertPack;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.resource.MeasureUnit;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.CommandType;
import io.subutai.plugin.storm.impl.Commands;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.storm.impl.StormService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class StormAlertListener implements AlertListener
{
    public static final String STORM_ALERT_LISTENER = "STORM_ALERT_LISTENER";
    private static final Logger LOG = LoggerFactory.getLogger( StormAlertListener.class.getName() );
    private static double MAX_RAM_QUOTA_MB;
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
	public void onAlert (AlertPack metric) throws Exception
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
        Environment environment = storm.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getHostname().equalsIgnoreCase( metric.getContainerId () ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getContainerId () ),
                    null );
        }

        //check if source host belongs to found storm cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Storm cluster", metric.getContainerId () ) );
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
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining Storm process PID", e );
        }

        //get Storm process resource usage by Storm pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( stormPID );

        //confirm that Storm is causing the stress, otherwise no-op
        MonitoringSettings thresholds = storm.getAlertSettings();
		QuotaAlertResource resource = (QuotaAlertResource) metric.getResource ();
        double ramLimit = resource.getValue ().getCurrentValue ().getValue (MeasureUnit.MB).doubleValue () * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
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

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        /*MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.8;

        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedByStorm )
            {
                //read current RAM quota
                int ramQuota = sourceHost.getRamQuota();

                // if available quota on resource host is greater than 10 % of calculated increase amount,
                // increase quota, otherwise scale horizontally
                int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                if ( MAX_RAM_QUOTA_MB > newRamQuota )
                {

                    LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                            sourceHost.getRamQuota(), newRamQuota );
                    //we can increase RAM quota
                    sourceHost.setRamQuota( newRamQuota );

                    quotaIncreased = true;
                }
            }

            if ( isCpuStressedByStorm )
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();

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
                //TODO adding the following line for testing purposes, remove it after that
                storm.addNode( targetCluster.getClusterName() );
                return;
            }

            // add new node
            storm.addNode( targetCluster.getClusterName() );
        }
        else
        {
            notifyUser();
        }*/
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
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


    /*@Override
    public String getSubscriberId()
    {
        return STORM_ALERT_LISTENER;
    }*/


    protected String parseService( String output, String target ) throws AlertException
    {
        Matcher m = Pattern.compile( "(?m)^.*$" ).matcher( output );
        if ( m.find() )
        {
            if ( m.group().toLowerCase().contains( target.toLowerCase() ) )
            {
                return m.group();
            }
        }
        else
        {
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return null;
    }


	@Override
	public String getTemplateName ()
	{
		return "storm";
	}
}

