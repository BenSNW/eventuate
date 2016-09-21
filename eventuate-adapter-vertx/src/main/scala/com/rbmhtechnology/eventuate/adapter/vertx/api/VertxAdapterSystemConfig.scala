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

import scala.collection.immutable.Seq

object VertxAdapterSystemConfig {

  def apply(): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(Seq.empty, Seq.empty)

  private def apply(adapterConfigurations: Seq[VertxAdapterConfig], codecClasses: Seq[Class[_]]): VertxAdapterSystemConfig = {
    validateConfigurations(adapterConfigurations) match {
      case Right(cs) =>
        new VertxAdapterSystemConfig(cs, codecClasses)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid configuration given. Cause: $err")
    }
  }

  private def validateConfigurations(configs: Seq[VertxAdapterConfig]): Either[String, Seq[VertxAdapterConfig]] = {
    for {
      _ <- validateConfigurationIds(configs).right
      _ <- validateLogWriterConfigurationEndpoints(configs).right
    } yield configs
  }

  private def validateConfigurationIds(configs: Seq[VertxAdapterConfig]): Either[String, Seq[VertxAdapterConfig]] = {
    val invalid = configs.groupBy(_.id).filter(c => c._2.size > 1)

    if (invalid.isEmpty)
      Right(configs)
    else
      Left(s"Ambigious definition for adapter(s) [${invalid.keys.map(id => s"'$id'").mkString(", ")}] given. " +
        s"An id must be used uniquely for a single adapter.")
  }

  private def validateLogWriterConfigurationEndpoints(configs: Seq[VertxAdapterConfig]): Either[String, Seq[VertxAdapterConfig]] = {
    val invalid = configs.collect({ case c: LogWriterConfig => c }).flatMap(_.endpoints).groupBy(identity).filter(c => c._2.size > 1)

    if (invalid.isEmpty)
      Right(configs)
    else
      Left(s"Source-endpoint(s) [${invalid.keys.map(e => s"'$e'").mkString(",")}] were configured multiple times. " +
        s"A source-endpoint may only be configured once.")
  }
}

class VertxAdapterSystemConfig(private[vertx] val configurations: Seq[VertxAdapterConfig], private[vertx] val codecClasses: Seq[Class[_]]) {
  val vertxWriterConfigurations: Seq[VertxWriterConfig] =
    configurations.collect({ case c: VertxWriterConfig => c })

  val logWriterConfigurations: Seq[LogWriterConfig] =
    configurations.collect({ case c: LogWriterConfig => c })

  def addAdapter(adapterConfiguration: VertxAdapterConfig): VertxAdapterSystemConfig =
    addAdapters(Seq(adapterConfiguration))

  def addAdapters(adapterConfigurations: Seq[VertxAdapterConfig]): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(configurations ++ adapterConfigurations, codecClasses)

  def registerDefaultCodecFor(first: Class[_], rest: Class[_]*): VertxAdapterSystemConfig =
    registerDefaultCodecForAll(first +: rest.toVector)

  def registerDefaultCodecForAll(classes: Seq[Class[_]]): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(configurations, codecClasses ++ classes)
}
