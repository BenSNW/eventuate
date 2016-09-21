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

import akka.actor.{ ActorRef, Props }
import com.rbmhtechnology.eventuate.EventsourcedActor
import io.vertx.core.eventbus.Message

import scala.util.{ Failure, Success }

private[vertx] object LogWriter {

  case class PersistMessage(message: Message[Any])

  def props(id: String, eventLog: ActorRef): Props =
    Props(new LogWriter(id, eventLog))
}

private[vertx] class LogWriter(val id: String, val eventLog: ActorRef) extends EventsourcedActor {
  import LogWriter._

  override def stateSync: Boolean = false

  override def onCommand: Receive = {
    case PersistMessage(msg) =>
      persist(msg.body()) {
        case Success(res) => msg.reply(res)
        case Failure(err) => msg.fail(0, err.getMessage)
      }
  }

  override def onEvent: Receive = {
    case _ =>
  }
}
