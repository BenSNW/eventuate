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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ Actor, ActorRef, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.LogRouter.LogRoute
import com.rbmhtechnology.eventuate.adapter.vertx.LogWriter.PersistMessage
import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ Message, MessageConsumer }

object LogRouter {

  case class Writer(id: String, log: ActorRef)
  case class LogRoute(sourceEndpoint: String, writer: Writer, filter: PartialFunction[Any, Boolean] = { case _ => true })

  def props(routes: Seq[LogRoute], vertx: Vertx): Props =
    Props(new LogRouter(routes, vertx))
}

class LogRouter(routes: Seq[LogRoute], vertx: Vertx) extends Actor {

  import VertxHandlerConverters._

  val writers = routes
    .groupBy(_.writer)
    .map { case (writer, _) => writer.id -> context.actorOf(LogWriter.props(writer.id, writer.log)) }

  val consumers = routes
    .map { r => installMessageConsumer(r.sourceEndpoint, writers(r.writer.id), r.filter) }

  private def installMessageConsumer(endpoint: String, writer: ActorRef, filter: PartialFunction[Any, Boolean]): MessageConsumer[Any] = {
    val handler = (msg: Message[Any]) => {
      if (filter.applyOrElse(msg.body(), (_: Any) => false)) {
        writer ! PersistMessage(msg)
      } else {
        msg.reply(msg.body)
      }
    }
    vertx.eventBus().consumer[Any](endpoint, handler.asVertxHandler)
  }

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = {
    consumers.foreach(_.unregister())
  }
}