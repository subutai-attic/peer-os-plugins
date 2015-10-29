package io.subutai.plugin.generic.impl.model;


import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.google.common.collect.Lists;

import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


@Entity
@Table( name = "plugin_profile" )
@Access( AccessType.FIELD )
public class ProfileEntity implements Profile
{
    @Id
    @Column( name = "id" )
    @GeneratedValue( strategy = GenerationType.AUTO )
    private Long id;

    @Column( name = "name" )
    private String name;

    @OneToMany( mappedBy = "profile", fetch = FetchType.EAGER, cascade = CascadeType.ALL, targetEntity =
            OperationEntity.class, orphanRemoval = true )
    private List<Operation> operations = Lists.newArrayList();


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


    public List<Operation> getOperations()
    {
        return operations;
    }


    public void setOperations( final List<Operation> operations )
    {
        this.operations = operations;
    }
}
