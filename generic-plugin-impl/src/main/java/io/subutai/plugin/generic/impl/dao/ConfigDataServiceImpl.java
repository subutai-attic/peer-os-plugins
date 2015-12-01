package io.subutai.plugin.generic.impl.dao;


import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;

import com.google.common.collect.Lists;

import io.subutai.common.dao.DaoManager;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.impl.model.OperationEntity;
import io.subutai.plugin.generic.impl.model.ProfileEntity;


public class ConfigDataServiceImpl implements ConfigDataService
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigDataServiceImpl.class );
    private DaoManager daoManager;


    public ConfigDataServiceImpl( final DaoManager daoManager )
    {
        this.daoManager = daoManager;
    }


    @Override
    public void saveProfile( final String profileName )
    {
        Profile profile = new ProfileEntity();
        profile.setName( profileName );

        EntityManager em = daoManager.getEntityManagerFactory().createEntityManager();

        try
        {
            daoManager.startTransaction( em );
            em.merge( profile );
            daoManager.commitTransaction( em );
        }
        catch ( Exception e )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService saveProfile:" + e.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public List<Profile> getAllProfiles()
    {
        List<Profile> result = Lists.newArrayList();

        EntityManager em = daoManager.getEntityManagerFromFactory();
        try
        {
            result = ( List<Profile> ) em.createQuery( "select h from ProfileEntity h" ).getResultList();
        }
        catch ( Exception e )
        {
            LOG.error( e.toString(), e );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
        return result;
    }


    @Override
    public void saveOperation( final Long profileId, final String operationName, final String commandName,
                               final String cwd, final String timeout, final Boolean daemon, final Boolean fromFile )
    {
        String parsedString = commandName.replaceAll( "\r", "" );
        byte[] encodedBytes = Base64.encodeBase64( parsedString.getBytes() );

        Operation operation = new OperationEntity();
        operation.setProfileId( profileId );
        operation.setOperationName( operationName );
        operation.setCommandName( new String( encodedBytes ) );
        operation.setCwd( cwd );
        operation.setTimeout( timeout );
        operation.setDaemon( daemon );
        operation.setScript( fromFile );

        EntityManager em = daoManager.getEntityManagerFactory().createEntityManager();

        try
        {
            daoManager.startTransaction( em );
            em.merge( operation );
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService saveOperation:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public List<Operation> getOperations( final String profileName )
    {
        List<Operation> result = Lists.newArrayList();

        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            query = em.createQuery( "select h from OperationEntity h where h.profileName = :profileName" );
            query.setParameter( "profileName", profileName );
            result = ( List<Operation> ) query.getResultList();
        }
        catch ( Exception e )
        {
            LOG.error( e.toString(), e );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
        return result;
    }


    @Override
    public List<Operation> getOperations( final Long profileId )
    {
        List<Operation> result = Lists.newArrayList();

        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            query = em.createQuery( "select h from OperationEntity h where h.profileId = :profileId" );
            query.setParameter( "profileId", profileId );
            result = ( List<Operation> ) query.getResultList();
        }
        catch ( Exception e )
        {
            LOG.error( e.toString(), e );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
        return result;
    }


    @Override
    public Operation getOperationByName( final String operationName )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            query = em.createQuery( "select e from OperationEntity e where e.operationName = :operationName" );
            query.setParameter( "operationName", operationName );
            return ( Operation ) query.getSingleResult();
        }
        catch ( Exception e )
        {
            daoManager.closeEntityManager( em );
            return null;
        }
    }


    @Override
    public Operation getOperationById( final Long operationId )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();
        try
        {
            daoManager.startTransaction( em );
            OperationEntity entity = em.find( OperationEntity.class, operationId );
            return entity;
        }
        catch ( Exception e )
        {
            daoManager.closeEntityManager( em );
            return null;
        }
    }


    @Override
    public Operation getOperationByCommand( final String commandName )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            query = em.createQuery( "select e from OperationEntity e where e.commandName = :commandName" );
            query.setParameter( "commandName", commandName );
            return ( Operation ) query.getSingleResult();
        }
        catch ( Exception e )
        {
            daoManager.closeEntityManager( em );
            return null;
        }
    }


    @Override
    public void updateOperation( final Long operationId, final String commandValue, final String cwdValue,
                                 final String timeoutValue, final Boolean daemonValue, final Boolean fromFile,
                                 final String operationName )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();

        byte[] encodedBytes = Base64.encodeBase64( commandValue.getBytes() );

        try
        {
            daoManager.startTransaction( em );
            OperationEntity entity = em.find( OperationEntity.class, operationId );
            entity.setCommandName( new String( encodedBytes ) );
            entity.setCwd( cwdValue );
            entity.setTimeout( timeoutValue );
            entity.setDaemon( daemonValue );
            entity.setScript( fromFile );
            entity.setOperationName( operationName );
            em.merge( entity );
            em.flush();
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService updateOperation:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public void deleteOperation( final Long operationId )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();

        try
        {
            daoManager.startTransaction( em );
            OperationEntity entity = em.find( OperationEntity.class, operationId );
            em.remove( entity );
            em.flush();
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService deleteOperation:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public void deleteProfile( final String profileName )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            daoManager.startTransaction( em );
            query = em.createQuery( "select e from ProfileEntity e where e.name = :profileName" );
            query.setParameter( "profileName", profileName );
            Profile profile = ( Profile ) query.getSingleResult();
            ProfileEntity entity = em.find( ProfileEntity.class, profile.getId() );
            em.remove( entity );
            em.flush();
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService deleteOperation:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public void deleteProfile( final Long profileId )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();

        try
        {
            daoManager.startTransaction( em );
            ProfileEntity entity = em.find( ProfileEntity.class, profileId );
            em.remove( entity );
            em.flush();
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService deleteOperation:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }


    @Override
    public void deleteOperations( final Long profileId )
    {
        EntityManager em = daoManager.getEntityManagerFromFactory();
        Query query;
        try
        {
            daoManager.startTransaction( em );
            query = em.createQuery( "delete from OperationEntity e where e.profileId = :profileId" );
            query.setParameter( "profileId", profileId );
            query.executeUpdate();
            daoManager.commitTransaction( em );
        }
        catch ( Exception e )
        {
            daoManager.closeEntityManager( em );
        }
    }
}
