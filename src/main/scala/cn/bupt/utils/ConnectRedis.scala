package cn.bupt.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object ConnectRedis extends App {

  val jedisPool = new JedisPool(new GenericObjectPoolConfig(),"192.168.25.145",6379)

  def getRedis() = jedisPool.getResource()
}
