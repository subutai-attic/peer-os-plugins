package org.safehaus.subutai.plugin.storm.impl;


import java.util.logging.Logger;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;


public class ClusterConfiguration
{
    private static final Logger LOG = Logger.getLogger( ClusterConfiguration.class.getName() );
    private TrackerOperation po;
    private StormImpl stormManager;


    public ClusterConfiguration( final TrackerOperation operation, final StormImpl stormManager )
    {
        this.po = operation;
        this.stormManager = stormManager;
    }

    //TODO use host.getInterfaces instead of Agents
    public void configureCluster( StormClusterConfiguration config, Environment environment )
            throws ClusterConfigurationException
    {
        po.addLogFailed( "Not implemented yet !!!");
    }
}
