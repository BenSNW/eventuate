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

import com.rbmhtechnology.eventuate.EventsourcedWriter
import com.rbmhtechnology.eventuate.adapter.vertx.api.EndpointRouter
import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ DeliveryOptions, Message }

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future, Promise }

case class MessageEnvelope(address: String, msg: Any)

trait MessagePublisher {
  def publish(address: String, msg: Any): Unit
}

trait MessageSender {
  def send[A](address: String, msg: Any, timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[A]
}

trait MessageDelivery {
  def deliver(messages: Seq[MessageEnvelope])(implicit ec: ExecutionContext): Future[Unit]
}

trait AtMostOnceDelivery extends MessageDelivery with MessagePublisher {
  override def deliver(messages: Seq[MessageEnvelope])(implicit ec: ExecutionContext): Future[Unit] =
    Future(messages.foreach(e => publish(e.address, e.msg)))
}

trait AtLeastOnceDelivery extends MessageDelivery with MessageSender {
  def confirmationTimeout: FiniteDuration

  override def deliver(messages: Seq[MessageEnvelope])(implicit ec: ExecutionContext): Future[Unit] =
    Future.sequence(messages.map(e => send[Unit](e.address, e.msg, confirmationTimeout))).map(_ => Unit)
}

trait VertxMessageProducer {
  def vertx: Vertx
}

trait VertxMessagePublisher extends VertxMessageProducer with MessagePublisher {
  override def publish(address: String, msg: Any): Unit =
    vertx.eventBus().publish(address, msg)
}

trait VertxMessageSender extends VertxMessagePublisher with MessageSender {
  import VertxHandlerConverters._

  override def send[A](address: String, msg: Any, timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[A] = {
    val promise = Promise[Message[A]]
    vertx.eventBus().send(address, msg, new DeliveryOptions().setSendTimeout(timeout.toMillis), promise.asVertxHandler)
    promise.future.map(_.body)
  }
}

trait VertxWriter[R, W] extends EventsourcedWriter[R, W] with MessageDelivery with ProgressStore[R, W] {
  import context.dispatcher

  def endpointRouter: EndpointRouter

  var messages: Vector[MessageEnvelope] = Vector.empty

  override def onCommand: Receive = {
    case _ =>
  }

  override def onEvent: Receive = {
    case ev =>
      messages = endpointRouter.endpoint(ev) match {
        case Some(endpoint) => messages :+ MessageEnvelope(endpoint, ev)
        case None           => messages
      }
  }

  override def write(): Future[W] = {
    val snr = lastSequenceNr
    val ft = deliver(messages).flatMap(x => writeProgress(id, snr))

    messages = Vector.empty
    ft
  }

  override def read(): Future[R] =
    readProgress(id)

  override def readSuccess(result: R): Option[Long] =
    Some(progress(result) + 1L)
}
