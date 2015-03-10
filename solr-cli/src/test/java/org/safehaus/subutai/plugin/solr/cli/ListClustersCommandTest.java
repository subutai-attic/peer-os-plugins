package org.safehaus.subutai.plugin.solr.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.solr.api.Solr;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Solr solr;
    @Mock
    SolrClusterConfig solrClusterConfig;


    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setSolrManager( solr );
    }


    @Test
    public void testGetPrestoManager() throws Exception
    {
        listClustersCommand.getSolrManager();

        // assertions
        assertNotNull( listClustersCommand.getSolrManager() );
        assertEquals( solr, listClustersCommand.getSolrManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( solr.getClusters() ).thenReturn( myList );
        when( solrClusterConfig.getClusterName() ).thenReturn( "testPresto" );

        listClustersCommand.doExecute();

        // assertions
        assertNotNull( solr.getClusters() );
    }


    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        when( solr.getClusters() ).thenReturn( myList );

        listClustersCommand.doExecute();

        // assertions
        assertTrue( solr.getClusters().isEmpty() );
    }
}