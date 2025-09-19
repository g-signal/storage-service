/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.storageservice.metrics;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.util.Resources;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;

@ExtendWith(DropwizardExtensionsSupport.class)
class LogstashTcpSocketAppenderFactoryIntegrationTest {

  @SuppressWarnings("unused")
  private static final DropwizardAppExtension<Configuration> EXTENSION =
    new DropwizardAppExtension<>(TestApplication.class, Resources.getResource("config/log-factory-test.yml").getPath());

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration,
        final Environment environment) {
    }
  }

  @Test
  void testLogging() throws Exception {
    // We don't actually have to do anything; this is checking for a failure
    // caused by a bug in some versions of Logback that causes a failure in
    // the LogstashTcpSocketAppenderFactory construction during Dropwizard startup
  }

}
