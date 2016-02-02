package io.subutai.plugin.generic.impl.model;


import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import io.subutai.plugin.generic.api.model.Profile;


@Entity
@Table( name = "plugin_profile" )
@Access( AccessType.FIELD )
public class ProfileEntity implements Profile
{
    @Id
    @Column( name = "id" )
    @GeneratedValue( strategy = GenerationType.IDENTITY )
    private Long id;

    @Column( name = "name", unique = true )
    private String name;


    public Long getId()
    {
        return id;
    }


    public void setId( final Long id )
    {
        this.id = id;
    }


    public String getName()
    {
        return name;
    }


    public void setName( final String name )
    {
        this.name = name;
    }
}
