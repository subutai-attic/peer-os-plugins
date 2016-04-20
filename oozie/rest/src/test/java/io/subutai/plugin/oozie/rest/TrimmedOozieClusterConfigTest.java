package io.subutai.plugin.oozie.rest;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.plugin.oozie.rest.TrimmedOozieClusterConfig;


@RunWith( MockitoJUnitRunner.class )
public class TrimmedOozieClusterConfigTest
{
    private TrimmedOozieClusterConfig trimmedOozieClusterConfig;
    @Before
    public void setUp() throws Exception
    {
        trimmedOozieClusterConfig = new TrimmedOozieClusterConfig();
    }


    @Test
    public void testGetClusterName() throws Exception
    {
        trimmedOozieClusterConfig.getClusterName();
    }


    @Test
    public void testGetHadoopClusterName() throws Exception
    {
        trimmedOozieClusterConfig.getHadoopClusterName();
    }


    @Test
    public void testGetServerHostname() throws Exception
    {
        trimmedOozieClusterConfig.getServerHostname();
    }


    @Test
    public void testGetClientHostNames() throws Exception
    {
        trimmedOozieClusterConfig.getClientHostNames();
    }
}