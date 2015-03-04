package org.safehaus.subutai.plugin.common.impl;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.safehaus.subutai.common.util.ServiceLocator;
import org.safehaus.subutai.core.identity.api.IdentityManager;
import org.safehaus.subutai.core.identity.api.User;
import org.safehaus.subutai.plugin.common.model.ClusterDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class PluginDataService
{
    private EntityManagerFactory emf;
    private Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();
    private static final Logger LOG = LoggerFactory.getLogger( PluginDataService.class );
    private IdentityManager identityManager;

    public PluginDataService( final EntityManagerFactory emf ) throws SQLException
    {
        init();
        this.emf = emf;
        try
        {
            this.emf.createEntityManager().close();
        }
        catch ( Exception e )
        {
            throw new SQLException( e );
        }
    }


    public PluginDataService( final EntityManagerFactory emf, final GsonBuilder gsonBuilder )
    {
        Preconditions.checkNotNull( emf, "EntityManagerFactory cannot be null." );
        Preconditions.checkNotNull( gsonBuilder, "GsonBuilder cannot be null." );
        init();
        this.emf = emf;
        gson = gsonBuilder.setPrettyPrinting().disableHtmlEscaping().create();
    }


    public void init()
    {
        ServiceLocator serviceLocator = new ServiceLocator();
        try {
            identityManager = serviceLocator.getService( IdentityManager.class );
        }
        catch ( NamingException e ) {
            LOG.error( e.getMessage() );
            e.printStackTrace();
        }
    }

    public void update( String source, String key, final Object info ) throws SQLException
    {
        String infoJson = gson.toJson( info );
        EntityManager em = emf.createEntityManager();
        Long userId = null;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            key = key.toUpperCase();
            em.getTransaction().begin();
            ClusterDataEntity entity = new ClusterDataEntity( source, key, infoJson, userId );
            em.merge( entity );
            em.flush();
            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
    }


    public void update( String source, String key, final String info ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        Long userId = null;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            key = key.toUpperCase();
            em.getTransaction().begin();
            ClusterDataEntity entity = new ClusterDataEntity( source, key, info, userId );
            em.merge( entity );
            em.flush();
            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
    }


    public <T> List<T> getInfo( String source, final Class<T> clazz ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        List<T> result = new ArrayList<>();
        Long userId;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
            return null;
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            em.getTransaction().begin();

            List<String> infoList =
                    em.createQuery( "select cd.info from ClusterDataEntity cd where cd.source = :source " +
                            "and cd.userId = :userId", String.class )
                            .setParameter( "source", source )
                            .setParameter( "userId", userId )
                            .getResultList();
            for ( final String info : infoList )
            {
                result.add( gson.fromJson( info, clazz ) );
            }

            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
        return result;
    }


    public <T> T getInfo( String source, String key, final Class<T> clazz ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        T result = null;
        Long userId;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
            return null;
        }

        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            key = key.toUpperCase();
            em.getTransaction().begin();
            TypedQuery<String> query = em.createQuery(
                    "select cd.info from ClusterDataEntity cd where cd.source = :source and cd.id = :id " +
                            "and cd.userId = :userId",
                    String.class );
            query.setParameter( "source", source );
            query.setParameter( "id", key );
            query.setParameter( "userId", userId );

            List<String> infoList = query.getResultList();
            if ( infoList.size() > 0 )
            {
                result = gson.fromJson( infoList.get( 0 ), clazz );
            }
            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
        return result;
    }


    public List<String> getInfo( String source ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        List<String> result = new ArrayList<>();
        Long userId;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
            return null;
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            em.getTransaction().begin();

            result =
                    em.createQuery( "select cd.info from ClusterDataEntity cd where cd.source = :source " +
                            "and cd.userId = :userId", String.class )
                            .setParameter( "source", source )
                            .setParameter( "userId", userId )
                            .getResultList();

            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
        return result;
    }


    public String getInfo( String source, String key ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        String result = null;
        Long userId;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
            return null;
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            key = key.toUpperCase();
            em.getTransaction().begin();
            TypedQuery<String> query = em.createQuery(
                    "select cd.info from ClusterDataEntity cd where cd.source = :source and cd.id = :id " +
                            "and cd.userId = :userId",
                    String.class );
            query.setParameter( "source", source );
            query.setParameter( "id", key );
            query.setParameter( "userId", userId );

            List<String> infoList = query.getResultList();
            if ( infoList.size() > 0 )
            {
                result = infoList.get( 0 );
            }
            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
        return result;
    }


    public void remove( String source, String key ) throws SQLException
    {
        EntityManager em = emf.createEntityManager();
        Long userId = null;
        try {
            userId = identityManager.getUser().getId();
        } catch ( Exception e ) {
            LOG.error( "Could not retrieve current user!" );
        }
        Preconditions.checkNotNull( userId, "UserId cannot be null." );
        try
        {
            source = source.toUpperCase();
            key = key.toUpperCase();
            em.getTransaction().begin();
            Query query =
                    em.createQuery( "DELETE FROM ClusterDataEntity cd WHERE cd.source = :source and cd.id = :id " +
                            "and cd.userId = :userId" );
            query.setParameter( "source", source );
            query.setParameter( "id", key );
            query.setParameter( "userId", userId );
            query.executeUpdate();
            em.getTransaction().commit();
        }
        catch ( Exception e )
        {
            if ( em.getTransaction().isActive() )
            {
                em.getTransaction().rollback();
            }
            throw new SQLException( e );
        }
        finally
        {
            em.close();
        }
    }


    public void setIdentityManager( IdentityManager identityManager ) {
        this.identityManager = identityManager;
    }


    public IdentityManager getIdentityManager() {
        return identityManager;
    }
}
