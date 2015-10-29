package io.subutai.plugin.generic.api.model;


import java.util.List;


public interface Profile
{
    public Long getId();

    public void setId( final Long id );

    public String getName();

    public void setName( final String name );

    public List<Operation> getOperations();

    public void setOperations( final List<Operation> operations );
}
