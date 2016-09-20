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
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ StorageProvider, VertxEndpointRouter }
import io.vertx.core.Vertx

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

case class EventEnvelope(address: String, event: Any)

trait MessageProducer {
  def vertx: Vertx
}

trait MessagePublisher extends MessageProducer {
  lazy val eventBusWriter: EventBusWriter = new EventBusPublisher(vertx)
}

trait MessageSender extends MessageProducer {
  lazy val eventBusWriter: EventBusSender = new EventBusSender(vertx)
}

trait MessageDelivery {
  def deliver(events: Seq[EventEnvelope])(implicit ec: ExecutionContext): Future[Unit]
}

trait AtMostOnceDelivery extends MessageDelivery {
  def eventBusWriter: EventBusWriter

  override def deliver(events: Seq[EventEnvelope])(implicit ec: ExecutionContext): Future[Unit] =
    Future(events.foreach(e => eventBusWriter.write(e.address, e.event)))
}

trait AtLeastOnceDelivery extends MessageDelivery {
  def eventBusWriter: EventBusSender
  def confirmationTimeout: FiniteDuration

  override def deliver(events: Seq[EventEnvelope])(implicit ec: ExecutionContext): Future[Unit] =
    Future.sequence(events.map(e => eventBusWriter.send[Unit](e.address, e.event, confirmationTimeout))).map(_ => Unit)
}

trait ProgressStore[R, W] {
  def writeProgress(id: String, snr: Long)(implicit executionContext: ExecutionContext): Future[W]

  def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[R]

  def progress(result: R): Long
}

trait SequenceNumberProgressStore extends ProgressStore[Long, Long] {
  def storageProvider: StorageProvider

  override def writeProgress(id: String, snr: Long)(implicit executionContext: ExecutionContext): Future[Long] =
    storageProvider.writeProgress(id, snr)

  override def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[Long] =
    storageProvider.readProgress(id)

  override def progress(result: Long): Long =
    result
}

trait VertxReader[R, W] extends EventsourcedWriter[R, W] with MessageDelivery with ProgressStore[R, W] {
  import context.dispatcher

  var events: Vector[EventEnvelope] = Vector.empty

  def endpointRouter: VertxEndpointRouter

  override def onCommand: Receive = {
    case _ =>
  }

  override def onEvent: Receive = {
    case ev =>
      events = endpointRouter.endpoint(ev) match {
        case Some(endpoint) => events :+ EventEnvelope(endpoint, ev)
        case None           => events
      }
  }

  override def write(): Future[W] = {
    val snr = lastSequenceNr
    val ft = deliver(events).flatMap(x => writeProgress(id, snr))

    events = Vector.empty
    ft
  }

  override def read(): Future[R] =
    readProgress(id)

  override def readSuccess(result: R): Option[Long] =
    Some(progress(result) + 1L)
}
