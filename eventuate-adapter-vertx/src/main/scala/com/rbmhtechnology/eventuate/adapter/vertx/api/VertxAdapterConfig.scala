/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.adapter.vertx.api

import akka.actor.ActorRef

import scala.concurrent.duration.FiniteDuration

sealed trait VertxAdapterConfig {
  def id: String
  def log: ActorRef
}
sealed trait VertxWriterConfig extends VertxAdapterConfig {
  def endpointRouter: EndpointRouter
}

case class VertxPublisherConfig(id: String, log: ActorRef, endpointRouter: EndpointRouter) extends VertxWriterConfig
case class VertxSenderConfig(id: String, log: ActorRef, endpointRouter: EndpointRouter, deliveryMode: DeliveryMode) extends VertxWriterConfig
case class LogWriterConfig(id: String, log: ActorRef, endpoints: Set[String], filter: PartialFunction[Any, Boolean]) extends VertxAdapterConfig

sealed trait ConfirmationType
case object Single extends ConfirmationType
case class Batch(size: Int) extends ConfirmationType

sealed trait DeliveryMode
case object AtMostOnce extends DeliveryMode
case class AtLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration) extends DeliveryMode

object EndpointRouter {

  def route(f: PartialFunction[Any, String]): EndpointRouter =
    new EndpointRouter(f)

  def routeAllTo(s: String): EndpointRouter =
    new EndpointRouter({ case _ => s })
}

class EndpointRouter(f: PartialFunction[Any, String]) {
  val endpoint: Any => Option[String] = f.lift
}

object VertxAdapterConfig {

  def fromLog(log: ActorRef): VertxWriterConfigFactory =
    new VertxWriterConfigFactory(log)

  def fromEndpoints(endpoints: String*): LogWriterConfigFactory =
    new LogWriterConfigFactory(endpoints.toSet)
}

trait CompletableVertxAdapterConfigFactory {
  def as(id: String): VertxAdapterConfig
}

class VertxWriterConfigFactory(log: ActorRef) {

  def publishTo(routes: PartialFunction[Any, String]): VertxPublisherConfigFactory =
    new VertxPublisherConfigFactory(log, EndpointRouter.route(routes))

  def sendTo(routes: PartialFunction[Any, String]): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, EndpointRouter.route(routes))
}

class VertxPublisherConfigFactory(log: ActorRef, endpoints: EndpointRouter)
  extends CompletableVertxAdapterConfigFactory {

  override def as(id: String): VertxAdapterConfig =
    VertxPublisherConfig(id, log, endpoints)
}

class VertxSenderConfigFactory(log: ActorRef, endpointRouter: EndpointRouter, deliveryMode: DeliveryMode = AtMostOnce)
  extends CompletableVertxAdapterConfigFactory {

  def atLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, endpointRouter, AtLeastOnce(confirmationType, confirmationTimeout))

  override def as(id: String): VertxAdapterConfig =
    VertxSenderConfig(id, log, endpointRouter, deliveryMode)
}

class LogWriterConfigFactory(endpoints: Set[String]) {

  def writeTo(log: ActorRef, filter: PartialFunction[Any, Boolean] = { case _ => true }) = new CompletableVertxAdapterConfigFactory {
    override def as(id: String): LogWriterConfig =
      LogWriterConfig(id, log, endpoints, filter)
  }
}
