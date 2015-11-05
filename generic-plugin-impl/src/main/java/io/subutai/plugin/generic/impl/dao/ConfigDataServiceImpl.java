package io.subutai.plugin.generic.impl.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.subutai.common.dao.DaoManager;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.impl.model.OperationEntity;
import io.subutai.plugin.generic.impl.model.ProfileEntity;


public class ConfigDataServiceImpl implements ConfigDataService
{
	private static final Logger LOG = LoggerFactory.getLogger (ConfigDataServiceImpl.class);
	private DaoManager daoManager;


	public ConfigDataServiceImpl (final DaoManager daoManager)
	{
		this.daoManager = daoManager;
	}


	@Override
	public void saveProfile (final String profileName)
	{
		Profile profile = new ProfileEntity();
		profile.setName(profileName);

		EntityManager em = daoManager.getEntityManagerFactory().createEntityManager();

		try
		{
			daoManager.startTransaction (em);
			em.merge (profile);
			daoManager.commitTransaction (em);
		}
		catch (Exception e)
		{
			daoManager.rollBackTransaction (em);
			LOG.error ("ConfigDataService saveProfile:" + e.toString());
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}
	}




	@Override
	public List <Profile> getAllProfiles()
	{
		List <Profile> result = Lists.newArrayList();

		EntityManager em = daoManager.getEntityManagerFromFactory();
		try
		{
			result = (List<Profile>) em.createQuery ("select h from ProfileEntity h").getResultList();
		}
		catch (Exception e)
		{
			LOG.error (e.toString(), e);
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}
		return result;
	}


	@Override
	public void saveOperation (final Long profileId, final String operationName, final String commandName, final String cwd, final String timeout, final Boolean daemon)
	{
		Operation operation = new OperationEntity();
		operation.setProfileId (profileId);
		operation.setOperationName (operationName);
		operation.setCommandName (commandName);
		operation.setCwd (cwd);
		operation.setTimeout (timeout);
		operation.setDaemon (daemon);

		EntityManager em = daoManager.getEntityManagerFactory().createEntityManager();

		try
		{
			daoManager.startTransaction (em);
			em.merge (operation);
			daoManager.commitTransaction (em);
		}
		catch (Exception ex)
		{
			daoManager.rollBackTransaction (em);
			LOG.error ("ConfigDataService saveOperation:" + ex.toString());
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}
	}


	@Override
	public List <Operation> getOperations (final Long profileId)
	{
		List <Operation> result = Lists.newArrayList();

		EntityManager em = daoManager.getEntityManagerFromFactory();
		Query query;
		try
		{
			// result = (List <Operation>) em.createQuery ("select h from OperationEntity h where h.profileId = :profileId").getResultList();
			query = em.createQuery ("select h from OperationEntity h where h.profileId = :profileId");
			query.setParameter ("profileId", profileId);
			result = (List<Operation>) query.getResultList();
		}
		catch (Exception e)
		{
			LOG.error (e.toString(), e);
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}
		return result;
	}


	@Override
	public boolean isOperationRegistered (final String operationName)
	{
		if (getOperationByName (operationName) == null)
		{
			return false;
		}
		else
		{
			return true;
		}
	}


	@Override
	public Operation getOperationByName (final String operationName)
	{
		EntityManager em = daoManager.getEntityManagerFromFactory();
		Query query;
		try
		{
			query = em.createQuery ("select e from OperationEntity e where e.operationName = :operationName");
			query.setParameter ("operationName", operationName);
			return (Operation) query.getSingleResult();
		}
		catch (Exception e)
		{
			daoManager.closeEntityManager (em);
			return null;
		}
	}


	@Override
	public void updateOperation (final Operation operation, final String commandValue, final String cwdValue, final String timeoutValue, final Boolean daemonValue)
	{
		EntityManager em = daoManager.getEntityManagerFromFactory();

		try
		{
			daoManager.startTransaction (em);
			OperationEntity entity = em.find (OperationEntity.class, operation.getOperationId());
			entity.setCommandName (commandValue);
			entity.setCwd (commandValue);
			entity.setTimeout (timeoutValue);
			entity.setDaemon (daemonValue);
			em.merge (entity);
			em.flush();
			daoManager.commitTransaction (em);
		}
		catch (Exception ex)
		{
			daoManager.rollBackTransaction (em);
			LOG.error ("ConfigDataService updateOperation:" + ex.toString());
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}
	}


	@Override
	public void deleteOperation( final Long operationId )
	{
		EntityManager em = daoManager.getEntityManagerFromFactory();

		try
		{
			daoManager.startTransaction (em);
			OperationEntity entity = em.find (OperationEntity.class, operationId);
			em.remove (entity);
			em.flush();
			daoManager.commitTransaction (em);
		}
		catch (Exception ex)
		{
			daoManager.rollBackTransaction (em);
			LOG.error ("ConfigDataService deleteOperation:" + ex.toString());
		}
		finally
		{
			daoManager.closeEntityManager (em);
		}

	}
}
