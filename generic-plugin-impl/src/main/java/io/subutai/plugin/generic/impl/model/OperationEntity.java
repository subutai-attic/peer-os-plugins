package io.subutai.plugin.generic.impl.model;


import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import io.subutai.plugin.generic.api.model.Profile;


@Entity
@Table( name = "plugin_operation" )
@Access( AccessType.FIELD )
public class OperationEntity
{
    @Column( name = "name" )
    private String name;

    @ManyToOne( targetEntity = ProfileEntity.class )
    @JoinColumn( name = "id" )
    private Profile profile;


    public String getName()
    {
        return name;
    }


    public void setName( final String name )
    {
        this.name = name;
    }


    public Profile getProfile()
    {
        return profile;
    }


    public void setProfile( final Profile profile )
    {
        this.profile = profile;
    }
}
