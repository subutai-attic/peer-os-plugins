package io.subutai.plugin.generic.impl.dao;


import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.dao.DaoManager;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Profile;


public class ConfigDataServiceImpl implements ConfigDataService
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigDataServiceImpl.class );
    private DaoManager daoManager;


    public ConfigDataServiceImpl( final DaoManager daoManager )
    {
        this.daoManager = daoManager;
    }


    @Override
    public void saveProfile( final Profile profile )
    {
        EntityManager em = daoManager.getEntityManagerFactory().createEntityManager();

        try
        {
            daoManager.startTransaction( em );
            daoManager.commitTransaction( em );
        }
        catch ( Exception ex )
        {
            daoManager.rollBackTransaction( em );
            LOG.error( "ConfigDataService saveProfile:" + ex.toString() );
        }
        finally
        {
            daoManager.closeEntityManager( em );
        }
    }
}
