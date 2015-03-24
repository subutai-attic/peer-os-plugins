package org.safehaus.subutai.plugin.elasticsearch.impl.alert;


import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find spark cluster by environment id
        List<ElasticsearchClusterConfiguration> clusters = elasticsearch.getClusters();

        ElasticsearchClusterConfiguration targetCluster = null;
        for ( ElasticsearchClusterConfiguration cluster : clusters )
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
        Environment environment =
                elasticsearch.getEnvironmentManager().findEnvironment( metric.getEnvironmentId() );
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
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to ES cluster", metric.getHost() ) );
            return;
        }

        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.5;

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
        double ramLimit = metric.getTotalRam() * thresholds.getRamAlertThreshold() / 100; // 0.8
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

            if ( isRamStressedByES ) {
                //read current RAM quota
                int ramQuota = sourceHost.getRamQuota();

                if ( ramQuota < MAX_RAM_QUOTA_MB ) {

                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                    if ( MAX_RAM_QUOTA_MB > newRamQuota ) {

                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.",
                                sourceHost.getHostname(), sourceHost.getRamQuota(), newRamQuota );
                        //we can increase RAM quota
                        sourceHost.setRamQuota( newRamQuota );

                        quotaIncreased = true;
                    }
                }
            }

            if ( isCpuStressedByES )
            {
                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();
                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT ) {
                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
                    LOG.info( "Increasing cpu quota of {} from {}% to {}%.",
                            sourceHost.getHostname(), cpuQuota, newCpuQuota );
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

            LOG.info( "Adding new node to {} cluster ...", targetCluster.getClusterName() );
            //filter out all nodes which already belong to ES cluster
            for ( Iterator<ContainerHost> iterator = containers.iterator(); iterator.hasNext(); )
            {
                final ContainerHost containerHost = iterator.next();
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


    @Override
    public String getSubscriberId()
    {
        return ES_ALERT_LISTENER;
    }
}
