package cn.bupt.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
object ConnectRedis {

  private val config = new GenericObjectPoolConfig()
  private val jedisPool = new JedisPool(config, "192.168.25.145", 6379, 3000,null,1)

  def getJedis() = jedisPool.getResource
}
