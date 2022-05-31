package utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis tools for reading/writing consumer offsets in Kafka
 */

object RedisUtil {

  var jedisPool : JedisPool = null

  /**
   * Get jedis from jedisPool, creat jedisPool if null
   * @return jedis
   */
  def getJedisFromPool() : Jedis = {
    if (jedisPool == null) {
      val jedisConfig = new JedisPoolConfig()
      jedisConfig.setMaxTotal(100)
      jedisConfig.setMaxIdle(20)
      jedisConfig.setMinIdle(20)
      jedisConfig.setBlockWhenExhausted(true)
      jedisConfig.setMaxWaitMillis(5000)
      jedisConfig.setTestOnBorrow(true)

      val host = PropsUtil(Config.REDIS_HOST)
      val port = PropsUtil(Config.REDIS_PORT).toInt

      jedisPool = new JedisPool(jedisConfig, host, port)
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis = getJedisFromPool()
    println(jedis)

    jedis.set("redis_test", "32,15,223,828")

    println(jedis.get("redis_test"))
  }

}
