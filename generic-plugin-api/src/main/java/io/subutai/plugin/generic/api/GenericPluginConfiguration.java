package io.subutai.plugin.generic.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class GenericPluginConfiguration
{
	private ArrayList <Profile> profiles = new ArrayList<>();

	public void addProfile (String newProfile)
	{
		Profile prof = new Profile(newProfile);
		this.profiles.add (prof);
		Collections.sort(this.profiles, new Comparator<Profile>()
		{
			@Override
			public int compare(Profile p1, Profile p2)
			{

				return p1.getName().compareTo(p2.getName());
			}
		});
	}

	public Profile findProfile (String profileName)
	{
		for (Profile p : this.profiles)
		{
			if (p.getName().equals (profileName))
			{
				return p;
			}
		}
		return null;
	}

	public void replaceProfile (Profile newProfile)
	{
		boolean found = false;
		for (Iterator<Profile> it = this.profiles.iterator(); it.hasNext();)
		{
			Profile p = it.next();
			if (p.getName().equals (newProfile.getName()))
			{
				it.remove();
				found = true;
				break;
			}
		}
		if (found)
		{
			this.profiles.add(newProfile);
			Collections.sort(this.profiles, new Comparator<Profile>()
			{
				@Override
				public int compare(Profile p1, Profile p2)
				{
					return p1.getName().compareTo(p2.getName());
				}
			});
		}
	}




	public ArrayList <Profile> getProfiles()
	{
		return this.profiles;
	}
}
