package io.subutai.plugin.generic.impl;


import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.impl.model.ProfileEntity;


/**
 * Created by ermek on 10/30/15.
 */
public class ConfigManager
{
    private Profile profile = new ProfileEntity();


    public void addProfile( final String profileName )
    {
        profile.setName( profileName );
    }
}
