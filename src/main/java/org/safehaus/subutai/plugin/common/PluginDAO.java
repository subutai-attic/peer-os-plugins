package org.safehaus.subutai.plugin.common;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.safehaus.subutai.core.identity.api.IdentityManager;
import org.safehaus.subutai.plugin.common.impl.EmfUtil;
import org.safehaus.subutai.plugin.common.impl.PluginDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * PluginDAO is used to manage cluster configuration information in database
 */
public class PluginDAO
{

    private static final Logger LOG = LoggerFactory.getLogger( PluginDAO.class.getName() );
    private Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();
    private EmfUtil emfUtil = new EmfUtil();

    private PluginDataService dataService;
    private IdentityManager identityManager;



    public PluginDAO( DataSource dataSource ) throws SQLException
    {
        this.dataService = new PluginDataService( emfUtil.getEmf(), identityManager );
    }


    public PluginDAO() throws SQLException
    {
    }


    public PluginDAO( final DataSource dataSource, final GsonBuilder gsonBuilder ) throws SQLException
    {
        Preconditions.checkNotNull( dataSource, "GsonBuilder is null" );
        this.dataService = new PluginDataService( emfUtil.getEmf(), identityManager , gsonBuilder );
    }


    protected void setupDb() throws SQLException
    {


    }


    public boolean saveInfo( String source, String key, Object info )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( key ), "Key is null or empty" );
        Preconditions.checkNotNull( info, "Info is null" );


        try
        {
            dataService.update( source, key, info );

            return true;
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return false;
    }


    public boolean saveInfo( String source, String key, String info )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( key ), "Key is null or empty" );
        Preconditions.checkNotNull( info, "Info is null" );


        try
        {
            dataService.update( source, key, info );

            return true;
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return false;
    }


    /**
     * Returns all POJOs from DB identified by source key
     *
     * @param source - source key
     * @param clazz - class of POJO
     *
     * @return - list of POJOs
     */
    public <T> List<T> getInfo( String source, Class<T> clazz )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkNotNull( clazz, "Class is null" );

        List<T> list = new ArrayList<>();
        try
        {
            list = dataService.getInfo( source, clazz );
        }
        catch ( JsonSyntaxException | SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return list;
    }


    /**
     * Returns POJO from DB
     *
     * @param source - source key
     * @param key - pojo key
     * @param clazz - class of POJO
     *
     * @return - POJO
     */
    public <T> T getInfo( String source, String key, Class<T> clazz )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( key ), "Key is null or empty" );
        Preconditions.checkNotNull( clazz, "Class is null" );

        try
        {
            return dataService.getInfo( source, key, clazz );
        }
        catch ( JsonSyntaxException | SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return null;
    }


    /**
     * Returns all POJOs from DB identified by source key
     *
     * @param source - source key
     *
     * @return - list of Json String
     */
    public List<String> getInfo( String source )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );

        List<String> list = new ArrayList<>();
        try
        {
            list = dataService.getInfo( source );
        }
        catch ( JsonSyntaxException | SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return list;
    }


    /**
     * Returns POJO from DB
     *
     * @param source - source key
     * @param key - pojo key
     *
     * @return - POJO
     */
    public String getInfo( String source, String key )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( key ), "Key is null or empty" );

        try
        {
            return dataService.getInfo( source, key );
        }
        catch ( JsonSyntaxException | SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return null;
    }


    /**
     * deletes POJO from DB
     *
     * @param source - source key
     * @param key - POJO key
     */
    public boolean deleteInfo( String source, String key )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( source ), "Source is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( key ), "Key is null or empty" );

        try
        {
            dataService.remove( source, key );
            return true;
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        return false;
    }


    public IdentityManager getIdentityManager() {
        return identityManager;
    }


    public void setIdentityManager( IdentityManager identityManager ) {
        this.identityManager = identityManager;
    }
}