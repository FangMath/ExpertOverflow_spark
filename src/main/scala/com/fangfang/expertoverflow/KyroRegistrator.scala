package com.fangfang.expertoverflow

import com.esotericsoftware.kryo.Kryo

class KyroRegistrator extends org.apache.spark.serializer.KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Post])
  }
}
