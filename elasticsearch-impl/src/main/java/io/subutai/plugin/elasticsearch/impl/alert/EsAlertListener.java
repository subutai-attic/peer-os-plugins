package io.subutai.plugin.elasticsearch.impl.alert;


import java.math.BigDecimal;
import java.util.Iterator;
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
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertResource;
import io.subutai.common.peer.AlertListener;
import io.subutai.common.peer.AlertPack;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.resource.MeasureUnit;
import io.subutai.common.resource.ResourceType;
import io.subutai.common.resource.ResourceValue;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


public class EsAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( EsAlertListener.class.getName() );

    public static final String ES_ALERT_LISTENER = "ES_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;

    private ElasticsearchImpl elasticsearch;


    public EsAlertListener( final ElasticsearchImpl elasticsearch )
    {
        this.elasticsearch = elasticsearch;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public String getTemplateName()
    {
        return ElasticsearchClusterConfiguration.TEMPLATE_NAME;
    }


    @Override
    public void onAlert( final AlertPack alert ) throws Exception
    {
        //find spark cluster by environment id
        List<ElasticsearchClusterConfiguration> clusters = elasticsearch.getClusters();

        ElasticsearchClusterConfiguration targetCluster = null;
        for ( ElasticsearchClusterConfiguration cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( alert.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", alert.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment = elasticsearch.getEnvironmentManager().loadEnvironment( alert.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", alert.getEnvironmentId() ), null );
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( alert.getContainerId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException(
                    String.format( "Alert source host %s not found in environment", alert.getContainerId() ), null );
        }

        //check if source host belongs to found cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to ES cluster", alert.getContainerId() ) );
            return;
        }

        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        //        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.5; @todo

        //figure out process pid
        int processPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( elasticsearch.getCommands().getStatusCommand(), sourceHost );
            processPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }

        //get process resource usage by pid
        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( processPID );

        //confirm that ES is causing the stress, otherwise no-op
        MonitoringSettings thresholds = elasticsearch.getAlertSettings();
        final QuotaAlertResource alertResource = ( QuotaAlertResource ) alert.getResource().getValue();
        final double currentRam = alertResource.getValue().getCurrentValue().getValue( MeasureUnit.MB ).doubleValue();
        double ramLimit = currentRam * ( ( double ) thresholds.getRamAlertThreshold() / 100 );
        double redLine = 0.7;
        boolean isCpuStressedByES = false;
        boolean isRamStressedByES = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressedByES = true;
        }
        else if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressedByES = true;
        }

        if ( !( isRamStressedByES || isCpuStressedByES ) )
        {
            LOG.info( "ES cluster ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedByES )
            {
                //read current RAM quota
                double ramQuota = sourceHost.getQuota( ResourceType.RAM ).getValue( MeasureUnit.MB ).doubleValue();

                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {

                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
                    {

                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                sourceHost.getQuota( ResourceType.RAM ).getValue( MeasureUnit.MB ).doubleValue(),
                                newRamQuota );
                        //we can increase RAM quota
                        ResourceValue quota = new ResourceValue( new BigDecimal( newRamQuota ), MeasureUnit.MB );
                        sourceHost.setQuota( ResourceType.RAM, quota );

                        quotaIncreased = true;
                    }
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            LOG.info( "Adding new node to {} cluster ...", targetCluster.getClusterName() );
            //filter out all nodes which already belong to ES cluster
            for ( Iterator<EnvironmentContainerHost> iterator = containers.iterator(); iterator.hasNext(); )
            {
                final EnvironmentContainerHost containerHost = iterator.next();
                if ( targetCluster.getNodes().contains( containerHost.getId() ) )
                {
                    iterator.remove();
                }
            }

            //no available nodes -> notify user
            if ( containers.isEmpty() )
            {
                notifyUser();
            }
            //add first available node
            else
            {
                //launch node addition process
                elasticsearch.addNode( targetCluster.getClusterName(), containers.iterator().next().getHostname() );
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
}
