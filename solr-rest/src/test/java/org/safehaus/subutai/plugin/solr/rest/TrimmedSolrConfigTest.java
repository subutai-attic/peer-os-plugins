package org.safehaus.subutai.plugin.solr.rest;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith( MockitoJUnitRunner.class )
public class TrimmedSolrConfigTest
{
    private TrimmedSolrConfig trimmedSolrConfig;

    @Before
    public void setUp() throws Exception
    {
        trimmedSolrConfig = new TrimmedSolrConfig();
    }


    @Test
    public void testGetClusterName() throws Exception
    {
        trimmedSolrConfig.getClusterName();
    }


    @Test
    public void testGetNumberOfNodes() throws Exception
    {
        trimmedSolrConfig.getNumberOfNodes();
    }
}