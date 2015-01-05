package org.safehaus.subutai.plugin.spark.impl.alert;


import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.metric.api.ResourceType;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
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


    public SparkAlertListener( final SparkImpl spark )
    {
        this.spark = spark;
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
            throwAlertException( String.format( "Cluster not found by environment id %s", metric.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment = spark.getEnvironmentManager().getEnvironmentByUUID( metric.getEnvironmentId() );
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
            }
        }
        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
                    null );
        }

        //check if source host belongs to found spark cluster
        if ( !targetCluster.getAllNodesIds().contains( sourceHost.getId() ) )
        {
            LOG.warn( String.format( "Alert source host %s does not belong to Spark cluster", metric.getHost() ) );
            return;
        }


        //figure out the offending resource type
        MonitoringSettings thresholds = spark.getAlertSettings();
        ResourceType offendingResource = null;
        if ( thresholds.getCpuAlertThreshold() > metric.getUsedCpu() )
        {
            //cpu is offending
            offendingResource = ResourceType.CPU;
        }
        else if ( thresholds.getDiskAlertThreshold() > metric.getUsedDisk() )
        {
            //disk is offending
            offendingResource = ResourceType.DISK;
        }
        else if ( thresholds.getRamAlertThreshold() > metric.getUsedRam() )
        {
            //ram is offending
            offendingResource = ResourceType.RAM;
        }

        if ( offendingResource == null )
        {
            LOG.warn( String.format( "Could not determine offending resource type from %s", metric ) );
            return;
        }

        int sparkPID;
        //figure out Spark process pid
        try
        {
            CommandResult result = commandUtil.execute( spark.getCommands().getObtainPidCommand(), sourceHost );
            //TODO actualize parsing of PID
           sparkPID = Integer.parseInt( result.getStdOut());
        }
        catch ( CommandException e )
        {
            throwAlertException( "Error obtaining Spark process PID", e );
        }


        //get Spark process offending resource usage by Spark pid

        //confirm that Spark is causing the stress, otherwise no-op


        //if cluster has auto-scaling enabled:
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it:
            //   yes -> increase quota limit
            //   no -> add new node (here if all nodes of underlying Hadoop are already used, then notify user)
        }
        else
        {
            //TODO find ways to notify user
            //if auto-scaling disabled -> notify user
        }
    }


    @Override
    public String getSubscriberId()
    {
        return SPARK_ALERT_LISTENER;
    }
}

