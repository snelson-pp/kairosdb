package org.kairosdb.core.http.rest;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class IngestModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		// Bind REST resource
		bind(IngestResource.class).in(Scopes.SINGLETON);
	}
}
