package io.subutai.plugin.hbase.cli;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DescribeClusterCommandTest
{
    private HBase hBase;
    private DescribeClusterCommand describeClusterCommand;
    private HBaseConfig hBaseConfig;


    @Before
    public void setUp() throws Exception
    {
        hBaseConfig = mock( HBaseConfig.class );
        hBase = mock( HBase.class );

        describeClusterCommand = new DescribeClusterCommand();
        describeClusterCommand.setHbaseManager( hBase );
    }


    @Test
    public void testGetHbaseManager() throws Exception
    {
        describeClusterCommand.setHbaseManager( hBase );
        describeClusterCommand.getHbaseManager();

        // assertions
        assertNotNull( describeClusterCommand.getHbaseManager() );
        assertEquals( hBase, describeClusterCommand.getHbaseManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        String id = UUID.randomUUID().toString();
        Set<String> mySet = mock( Set.class );
        mySet.add( id );
        Set<String> mySet2 = mock( Set.class );
        mySet2.add( id );
        describeClusterCommand.setHbaseManager( hBase );
        when( hBase.getCluster( anyString() ) ).thenReturn( hBaseConfig );
        when( hBaseConfig.getRegionServers() ).thenReturn( mySet );
        Iterator<String> iterator = mock( Iterator.class );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( id );
        Iterator<String> iterator2 = mock( Iterator.class );
        when( mySet2.iterator() ).thenReturn( iterator2 );
        when( iterator2.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator2.next() ).thenReturn( id );

        describeClusterCommand.doExecute();

        // assertions
        assertNotNull( hBase.getCluster( anyString() ) );
        verify( hBaseConfig ).getClusterName();
        verify( hBaseConfig ).getHbaseMaster();
    }


    @Test
    public void testDoExecuteWhenHbaseConfigIsNull()
    {
        when( hBase.getCluster( anyString() ) ).thenReturn( null );

        describeClusterCommand.doExecute();
    }
}