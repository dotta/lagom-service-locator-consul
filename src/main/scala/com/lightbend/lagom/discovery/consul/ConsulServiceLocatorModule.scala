package com.lightbend.lagom.discovery.consul

import com.ecwid.consul.v1.ConsulClient
import com.lightbend.lagom.javadsl.api.ServiceLocator

import javax.inject.{ Inject, Provider, Singleton }

import play.api.{ Configuration, Environment, Mode }
import play.api.inject.{ Binding, Module }

/**
 * This module binds the ServiceLocator interface from Lagom to the `ConsulServiceLocator`.
 * The `ConsulServiceLocator` is only bound if the application has been started in `Prod` mode.
 * In `Dev` mode the embedded service locator of Lagom is used.
 */
class ConsulServiceLocatorModule extends Module {

  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    if (environment.mode == Mode.Prod) Seq(
      bind[ServiceLocator].to[ConsulServiceLocator].in[Singleton],
      bind[ConsulClient].toProvider[ConsulServiceLocatorModule.ConsulClientProvider]
    )
    else Seq.empty
}

object ConsulServiceLocatorModule {
  private class ConsulClientProvider @Inject()(config: ConsulConfig) extends Provider[ConsulClient] {
    override lazy val get: ConsulClient = new ConsulClient(config.agentHostname, config.agentPort)
  }
}
