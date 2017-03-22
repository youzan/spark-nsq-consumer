/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.youzan.bigdata.streaming.nsq

/**
  * Created by chenjunzou on 2017/3/17.
  */

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._

import scala.collection.mutable.Map

/**
  * Input stream that pulls messages from a Nsq Broker.
  *
  * @param nsqParams Map of nsq configuration parameters.
  *                    see: http://doc.qima-inc.com/pages/viewpage.action?title=NSQ-Client-Java
  * @param storageLevel RDD storage level.
  */

class NsqInputDStream (
     _ssc: StreamingContext,
     nsqParams: Map[String, String],
     storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
  extends ReceiverInputDStream[NSQMessageWrapper](_ssc) with LazyLogging {

  def getReceiver() = new UnreliableNSQReceiver(nsqParams, storageLevel)
}

