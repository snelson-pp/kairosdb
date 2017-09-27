package org.kairosdb.core.http.rest;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class QueryModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		// Bind REST resource
		bind(QueryResource.class).in(Scopes.SINGLETON);
	}
}
