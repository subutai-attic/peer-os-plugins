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
import io.subutai.common.metric.ExceededQuota;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.hub.share.quota.ContainerCpuResource;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerRamResource;
import io.subutai.hub.share.quota.Quota;
import io.subutai.hub.share.resource.ByteUnit;
import io.subutai.hub.share.resource.ByteValueResource;
import io.subutai.hub.share.resource.ContainerResourceType;
import io.subutai.hub.share.resource.ResourceValue;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.Commands;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


public class EsAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( EsAlertListener.class.getName() );

    public static final String ES_ALERT_LISTENER = "ES_ALERT_LISTENER";
    private static final String HANDLER_ID = "DEFAULT_ELASTICSEARCH_EXCEEDED_QUOTA_ALERT_HANDLER";
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


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error( context, e );
        throw new AlertHandlerException( context, e );
    }


    protected int parsePid( String output ) throws AlertHandlerException
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
    public String getDescription()
    {
        return "Node resource for elasticsearch exceeded defined threshold.";
    }


    @Override
    public void process( Environment environment, QuotaAlertValue quotaAlertValue ) throws AlertHandlerException
    {
        //find spark cluster by environment id
        List<ElasticsearchClusterConfiguration> clusters = elasticsearch.getClusters();

        ElasticsearchClusterConfiguration targetCluster = null;
        for ( ElasticsearchClusterConfiguration cluster : clusters )
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
            return;
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( quotaAlertValue.getValue().getHostId().getId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not" + " found in environment",
                    quotaAlertValue.getValue().getHostId().getId() ), null );
            return;
        }

        //check if source host belongs to found cluster
        if ( targetCluster.getNodes() != null && !targetCluster.getNodes().contains(
                quotaAlertValue.getValue().getHostId().getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to ES cluster",
                    quotaAlertValue.getValue().getHostId().getId() ) );
            return;
        }

        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        //        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.5; @todo

        //figure out process pid
        int processPID = 0;
        try
        {
            CommandResult result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
            processPID = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }

        //get process resource usage by pid
        ProcessResourceUsage processResourceUsage = null;
        try
        {
            processResourceUsage = sourceHost.getProcessResourceUsage( processPID );

            //confirm that ES is causing the stress, otherwise no-op
            MonitoringSettings thresholds = elasticsearch.getAlertSettings();
            ExceededQuota exceededQuota = quotaAlertValue.getValue();
            ResourceValue<BigDecimal> currentValue = exceededQuota.getCurrentValue();
            double ramLimit =
                    currentValue.getValue().doubleValue() * ( ( double ) thresholds.getRamAlertThreshold() / 100 );
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
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    double ramQuota = containerQuota.get( ContainerResourceType.RAM ).getAsRamResource().getResource()
                                                    .getValue( ByteUnit.MB ).doubleValue();
                    //                double ramQuota = sourceHost.getQuota( ResourceType.RAM ).getValue( MeasureUnit
                    // .MB ).doubleValue();

                    if ( ramQuota < MAX_RAM_QUOTA_MB )
                    {

                        // if available quota on resource host is greater than 10 % of calculated increase amount,
                        // increase quota, otherwise scale horizontally
                        double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                        if ( MAX_RAM_QUOTA_MB > newRamQuota )
                        {

                            LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                    ramQuota, newRamQuota );
                            //we can increase RAM quota
                            Quota quota = new Quota( new ContainerRamResource( newRamQuota, ByteUnit.MB ),
                                    thresholds.getRamAlertThreshold() );

                            containerQuota.add( quota );
                            sourceHost.setQuota( containerQuota );

                            quotaIncreased = true;
                        }
                    }
                }

                if ( isCpuStressedByES )
                {

                    //read current CPU quota
                    final ContainerQuota containerQuota = sourceHost.getQuota();
                    ContainerCpuResource cpuQuota = containerQuota.get( ContainerResourceType.CPU ).getAsCpuResource();
                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );
                        //we can increase CPU quota
                        Quota quota =
                                new Quota( new ContainerCpuResource( newCpuQuota ), thresholds.getRamAlertThreshold() );

                        containerQuota.add( quota );
                        sourceHost.setQuota( containerQuota );

                        quotaIncreased = true;
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
        catch ( Exception e )
        {
            throwAlertException( "Error getting resource usage", e );
        }
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }
}
