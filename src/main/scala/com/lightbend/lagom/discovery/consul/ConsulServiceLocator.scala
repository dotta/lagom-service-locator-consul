package com.lightbend.lagom.discovery.consul

import java.io.File
import java.net.{InetAddress, URI}
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, CompletionStage}
import java.util.function.{Function => JFunction}
import javax.inject.Inject

import com.ecwid.consul.v1.catalog.model.CatalogService
import com.ecwid.consul.v1.{QueryParams, ConsulClient}
import com.lightbend.lagom.javadsl.api.ServiceLocator
import com.typesafe.config.ConfigException.BadValue
import play.api.{Configuration, Environment, Mode}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.convert.decorateAsScala._
import scala.collection.concurrent.Map
import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.util.{Random => JRandom}

object ConsulServiceLocator {
  val config = Configuration.load(Environment(new File("."), getClass.getClassLoader, Mode.Prod)).underlying
  val agentHostname = config.getString("lagom.discovery.consul.agent-hostname")
  val agentPort     = config.getInt("lagom.discovery.consul.agent-port")
  val scheme        = config.getString("lagom.discovery.consul.uri-scheme")
  val routingPolicy = config.getString("lagom.discovery.consul.routing-policy")
}

class ConsulServiceLocator @Inject()(implicit ec: ExecutionContext) extends ServiceLocator {
  import ConsulServiceLocator._

  private val client = new ConsulClient(agentHostname, agentPort)
  private val roundRobinIndexFor: Map[String, Int] = TrieMap.empty[String, Int]

  override def locate(name: String): CompletionStage[Optional[URI]] =
    locateAsScala(name).map(_.asJava).toJava

  override def doWithService[T](name: String, block: JFunction[URI, CompletionStage[T]]): CompletionStage[Optional[T]] =
    locateAsScala(name).flatMap { uriOpt =>
      uriOpt.fold(Future.successful(Optional.empty[T])) { uri =>
        block.apply(uri).toScala.map(Optional.of(_))
      }
    }.toJava

  private def locateAsScala(name: String): Future[Option[URI]] = Future {
    val instances = client.getCatalogService(name, QueryParams.DEFAULT).getValue.toList
    instances.size match {
      case 0 => None
      case 1 => toURIs(instances).headOption
      case _ =>
        routingPolicy match {
          case "first"       => Some(pickFirstInstance(instances))
          case "random"      => Some(pickRandomInstance(instances))
          case "round-robin" => Some(pickRoundRobinInstance(name, instances))
          case unknown       => throw new BadValue("lagom.discovery.consul.routing-policy", s"[$unknown] is not a valid routing algorithm")
        }
    }
  }

  private implicit object DefaultOrdering extends Ordering[URI] {
    override def compare(x: URI, y: URI): Int = x.compareTo(y)
  }

  private[consul] def pickFirstInstance(services: List[CatalogService]): URI = {
    assert(services.nonEmpty)
    toURIs(services).sorted.head
  }

  private[consul] def pickRandomInstance(services: List[CatalogService]): URI = {
    assert(services.nonEmpty)
    toURIs(services).sorted.get(JRandom.nextInt(services.size - 1))
  }

  private[consul] def pickRoundRobinInstance(name: String, services: List[CatalogService]): URI = {
    assert(services.nonEmpty)
    roundRobinIndexFor.putIfAbsent(name, 0)
    val sortedServices = toURIs(services).sorted
    val currentIndex = roundRobinIndexFor(name)
    val nextIndex =
      if (sortedServices.size > currentIndex + 1) currentIndex + 1
      else 0
    roundRobinIndexFor += (name -> nextIndex)
    sortedServices.get(currentIndex)
  }

  private def toURIs(services: List[CatalogService]): List[URI] =
    services.map { service =>
      val address = service.getServiceAddress
      val serviceAddress =
        if (address == "" || address == "localhost") InetAddress.getLoopbackAddress.getHostAddress
        else address
      new URI(s"$scheme://$serviceAddress:${service.getServicePort}")
    }
}
