package io.subutai.plugin.cassandra.rest;


import java.util.Map;

import io.subutai.plugin.cassandra.api.CassandraClusterConfig;


public class CassandraClusterConfigExtended extends CassandraClusterConfig
{
    private Map< String, String > nodes;
    private Map< String, String  > seedNodes;
}
