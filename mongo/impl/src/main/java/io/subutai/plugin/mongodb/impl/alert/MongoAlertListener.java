package io.subutai.plugin.mongodb.impl.alert;


import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

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
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.hub.share.quota.ContainerCpuResource;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerRamResource;
import io.subutai.hub.share.quota.Quota;
import io.subutai.hub.share.resource.ByteUnit;
import io.subutai.hub.share.resource.ByteValueResource;
import io.subutai.hub.share.resource.ContainerResourceType;
import io.subutai.hub.share.resource.ResourceValue;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.impl.MongoImpl;
import io.subutai.plugin.mongodb.impl.common.Commands;


public class MongoAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger( MongoAlertListener.class );
    public static final String MONGO_ALERT_LISTENER = "MONGO_ALERT_LISTENER";

    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;
    private static final String HANDLER_ID = "DEFAULT_MONGO_EXCEEDED_QUOTA_ALERT_HANDLER";
    private MongoImpl mongo;
    private CommandUtil commandUtil = new CommandUtil();


    public MongoAlertListener( final MongoImpl mongo )
    {
        this.mongo = mongo;
    }


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        throw new AlertHandlerException( context, e );
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find mongo cluster by environment id
        List<MongoClusterConfig> clusters = mongo.getClusters();

        MongoClusterConfig targetCluster = null;
        for ( MongoClusterConfig cluster : clusters )
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

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( quotaAlertValue.getValue().getHostId().toString() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found " + "in environment",
                    quotaAlertValue.getValue().getHostId() ), null );
            return;
        }

        //check if source host belongs to found mongo cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong " + "to Mongo cluster",
                    quotaAlertValue.getValue().getHostId() ) );
            return;
        }

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        //        MAX_RAM_QUOTA_MB = Float.parseFloat (sourceHost.getAvailableRamQuota().getValue()) * 0.8;

        //figure out Mongo  process pid
        int mongoPid = 0;
        try
        {
            CommandResult result = commandUtil.execute( Commands.getPidCommand(), sourceHost );
            mongoPid = parsePid( result.getStdOut() );
        }
        catch ( Exception e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }

        //get Zookeeper process resource usage by Mongo pid


        //auto-scaling is enabled -> scale cluster
        try
        {
            //get mongo process resource usage by mongo pid
            ProcessResourceUsage processResourceUsage =
                    mongo.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), mongoPid );

            //confirm that mongo is causing the stress, otherwise no-op
            MonitoringSettings thresholds = mongo.getAlertSettings();
            ExceededQuota exceededQuota = quotaAlertValue.getValue();
            ResourceValue<BigDecimal> currentValue = exceededQuota.getCurrentValue();

            double ramLimit =
                    currentValue.getValue().doubleValue() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
            double redLine = 0.9;
            boolean isCpuStressedByMongo = false;
            boolean isRamStressedByMongo = false;

            if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
            {
                isRamStressedByMongo = true;
            }
            if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
            {
                isCpuStressedByMongo = true;
            }

            if ( !( isRamStressedByMongo || isCpuStressedByMongo ) )
            {
                LOGGER.info( "mongo cluster runs ok" );
                return;
            }


            //auto-scaling is enabled -> scale cluster
            if ( targetCluster.isAutoScaling() )
            {
                // check if a quota limit increase does it
                boolean quotaIncreased = false;

                if ( isRamStressedByMongo )
                {
                    //read current RAM quota
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    double ramQuota = containerQuota.get( ContainerResourceType.RAM ).getAsRamResource().getResource()
                                                    .getValue( ByteUnit.MB ).doubleValue();
                    if ( ramQuota < MAX_RAM_QUOTA_MB )
                    {
                        double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_MB ) / 100;

                        if ( MAX_RAM_QUOTA_MB > newRamQuota )
                        {
                            LOGGER.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                    ramQuota, newRamQuota );

                            Quota quota = new Quota( new ContainerRamResource( newRamQuota, ByteUnit.MB ),
                                    thresholds.getRamAlertThreshold() );

                            containerQuota.add( quota );
                            sourceHost.setQuota( containerQuota );
                            quotaIncreased = true;
                        }
                    }
                }
                if ( isCpuStressedByMongo )
                {
                    //read current RAM quota
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    ContainerCpuResource cpuQuota = containerQuota.get( ContainerResourceType.CPU ).getAsCpuResource();

                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOGGER.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );

                        Quota q =
                                new Quota( new ContainerCpuResource( newCpuQuota ), thresholds.getRamAlertThreshold() );

                        containerQuota.add( q );
                        sourceHost.setQuota( containerQuota );
                        quotaIncreased = true;
                    }
                }

                //quota increase is made, return
                if ( quotaIncreased )
                {
                    return;
                }

                // add new node
                MongoClusterConfig mongoClusterConfig = mongo.getCluster( targetCluster.getClusterName() );
                if ( mongoClusterConfig == null )
                {
                    throwAlertException( String.format( "Mongo cluster %s not found", targetCluster.getClusterName() ),
                            null );
                    return;
                }

                Set<String> availableNodes = mongoClusterConfig.getAllNodes();
                availableNodes.removeAll( targetCluster.getAllNodes() );

                //no available nodes -> notify user
                if ( availableNodes.isEmpty() )
                {
                    notifyUser();
                }
                //add first available node
                else
                {
                    String newNodeId = availableNodes.iterator().next();
                    String newNodeHostName = null;
                    for ( EnvironmentContainerHost containerHost : containers )
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
                                String.format( "Could not obtain available mongo node from environment by id %s",
                                        newNodeId ), null );
                        return;
                    }

                    //launch node addition process
                    mongo.addNode( targetCluster.getClusterName(), newNodeHostName );
                }
            }
            else
            {
                notifyUser();
            }
        }
        catch ( Exception e )
        {
            throwAlertException( "Error processing threshold alert", e );
        }
    }


    /*@Override
    public String getSubscriberId()
    {
        return MONGO_ALERT_LISTENER;
    }*/


    protected int parsePid( String output ) throws Exception
    {
        int pid;
        output = output.replaceAll( "\n", "" );
        pid = Integer.parseInt( output );
        if ( pid == 0 )
        {
            throw new CommandException( "Couldn't parse pid" );
        }
        return pid;
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
        return "Node resource exceeded threshold alert handler for mongo cluster.";
    }
}
