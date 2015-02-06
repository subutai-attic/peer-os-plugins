package org.safehaus.subutai.plugin.mongodb.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by talas on 1/15/15.
 */
public class MongoAlertListener implements AlertListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( MongoAlertListener.class );
    private static final String MONGO_ALERT_LISTENER = "MONGO_ALERT_LISTENER";

    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;

    private MongoImpl mongo;
    private CommandUtil commandUtil = new CommandUtil();


    public MongoAlertListener( final MongoImpl mongo )
    {
        this.mongo = mongo;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {
        //find mongo cluster by environment id
        List<MongoClusterConfig> clusters = mongo.getClusters();

        MongoClusterConfig targetCluster = null;
        for ( MongoClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( containerHostMetric.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throw new Exception(
                    String.format( "Cluster not found by environment id %s", containerHostMetric.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment =
                mongo.getEnvironmentManager().findEnvironment( containerHostMetric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new Exception(
                    String.format( "Environment not found by id %s", containerHostMetric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<ContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( containerHostMetric.getHostId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new Exception(
                    String.format( "Alert source host %s not found in environment", containerHostMetric.getHost() ),
                    null );
        }

        //check if source host belongs to found mongo cluster
        if ( !targetCluster.getAllNodeIds().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to Mongo cluster",
                    containerHostMetric.getHost() ) );
            return;
        }

        //figure out Mongo process pid
        int zookeeperPid = 0;
        try
        {
            CommandResult result = commandUtil
                    .execute( new RequestBuilder( Commands.getMongodbServiceStatus().getCommand() ), sourceHost );
            zookeeperPid = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new Exception( "Error obtaining Mongo process PID", e );
        }

        //get Zookeeper process resource usage by Mongo pid
        ProcessResourceUsage processResourceUsage =
                mongo.getMonitor().getProcessResourceUsage( sourceHost, zookeeperPid );

        //confirm that Mongo is causing the stress, otherwise no-op
        MonitoringSettings thresholds = mongo.getAlertSettings();
        double ramLimit = containerHostMetric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.9;
        boolean cpuStressedByMongo = false;
        boolean ramStressedByMongo = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            ramStressedByMongo = true;
        }
        if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            cpuStressedByMongo = true;
        }

        if ( !ramStressedByMongo && !cpuStressedByMongo )
        {
            LOGGER.info( "Mongo cluster runs ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( ramStressedByMongo )
            {
                //read current RAM quota
                int ramQuota = mongo.getQuotaManager().getRamQuota( sourceHost.getId() );


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    //we can increase RAM quota
                    mongo.getQuotaManager().setRamQuota( sourceHost.getId(),
                            Math.min( MAX_RAM_QUOTA_MB, ramQuota + RAM_QUOTA_INCREMENT_MB ) );

                    quotaIncreased = true;
                }
            }
            if ( cpuStressedByMongo )
            {

                //read current CPU quota
                int cpuQuota = mongo.getQuotaManager().getCpuQuota( sourceHost.getId() );

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    //we can increase CPU quota
                    mongo.getQuotaManager().setCpuQuota( sourceHost.getId(),
                            Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT ) );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            // add new node
            MongoClusterConfig zookeeperClusterConfig = mongo.getCluster( targetCluster.getClusterName() );
            if ( zookeeperClusterConfig == null )
            {
                throw new Exception( String.format( "Mongo cluster %s not found", targetCluster.getClusterName() ),
                        null );
            }

            Set<UUID> availableNodes = zookeeperClusterConfig.getAllNodeIds();
            availableNodes.removeAll( targetCluster.getAllNodeIds() );

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
                    throw new Exception(
                            String.format( "Could not obtain available spark node from environment by id %s",
                                    newNodeId ), null );
                }

                //launch node addition process
                mongo.addNode( targetCluster.getClusterName(), NodeType.valueOf( sourceHost.getNodeGroupName() ) );
            }
        }
        else
        {
            notifyUser();
        }
    }


    @Override
    public String getSubscriberId()
    {
        return MONGO_ALERT_LISTENER;
    }


    protected int parsePid( String output ) throws Exception
    {
        Pattern p = Pattern.compile( "process\\s*(\\d+)", Pattern.CASE_INSENSITIVE );

        Matcher m = p.matcher( output );

        if ( m.find() )
        {
            return Integer.parseInt( m.group( 1 ) );
        }
        else
        {
            throw new Exception( String.format( "Could not parse PID from %s", output ), null );
        }
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
