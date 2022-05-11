package io.javalin.plugin.redis

import com.fasterxml.jackson.databind.ObjectMapper
import io.javalin.http.Context
import io.javalin.http.Cookie
import io.lettuce.core.api.StatefulRedisConnection
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletableFuture

private fun Context.redisSessionCookie(): String? =
    this.cookie(this.appAttribute<String>(RedisPlugin.ATTRIBUTE_COOKIE_NAME))

private fun Context.redisSessionConnection(): StatefulRedisConnection<String, ByteBuffer?> =
    appAttribute(RedisPlugin.ATTRIBUTE_CONNECTION_NAME)

private fun Context.redisBsonObjectMapper(): ObjectMapper = appAttribute(RedisPlugin.ATTRIBUTE_OBJECT_MAPPER_NAME)

fun <T> Context.session(clazz: Class<T>): T? {
    val connection = this.redisSessionConnection()
    val objectMapper = this.redisBsonObjectMapper()
    val cookie = this.redisSessionCookie()

    return cookie?.let {
        connection.sync().get(cookie)?.let { objectMapper.readValue(it.array(), clazz) }
    }
}

inline fun <reified T> Context.session(): T? = this.session(T::class.java)

fun <T> Context.sessionAsync(clazz: Class<T>): CompletableFuture<T?> {
    val connection = this.redisSessionConnection()
    val objectMapper = this.redisBsonObjectMapper()
    val cookie = this.redisSessionCookie()

    return cookie?.let {
        connection.async().get(cookie).toCompletableFuture().thenApplyAsync {
            it?.let { objectMapper.readValue(it.array(), clazz) }
        }
    } ?: CompletableFuture.completedFuture(null)
}

inline fun <reified T> Context.sessionAsync(): CompletableFuture<T?> = this.sessionAsync(T::class.java)

fun <T> Context.session(value: T?) {
    val connection = this.redisSessionConnection()
    var cookie = this.redisSessionCookie()

    if (value == null) {
        if (cookie != null) {
            connection.sync().del(cookie)
            this.removeCookie(this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_NAME))
        }
    } else {
        if (cookie == null) {
            cookie = UUID.randomUUID().toString()
            val cookieOptions: CookieOptions = this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_OPTIONS_NAME)
            this.cookie(
                Cookie(
                    this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_NAME),
                    cookie,
                    cookieOptions.path,
                    cookieOptions.maxAge,
                    cookieOptions.secure,
                    cookieOptions.version,
                    cookieOptions.isHttpOnly,
                    cookieOptions.comment,
                    cookieOptions.domain,
                    cookieOptions.sameSite
                )
            )
        }

        val objectMapper = this.redisBsonObjectMapper()
        val byteBuffer = ByteBuffer.wrap(objectMapper.writeValueAsBytes(value))
        val cookieOptions: CookieOptions = this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_OPTIONS_NAME)
        connection.sync().setex(cookie, cookieOptions.maxAge.toLong(), byteBuffer)
    }
}

fun <T> Context.sessionAsync(value: T?): CompletableFuture<Boolean> {
    val connection = this.redisSessionConnection()
    var cookie = this.redisSessionCookie()

    return if (value == null) {
        if (cookie != null) {
            connection.async().del(cookie).toCompletableFuture()
                .thenApply { it == 1L }
                .thenApply {
                    this.removeCookie(this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_NAME))
                    it
                }
        } else {
            CompletableFuture.completedFuture(true)
        }
    } else {
        if (cookie == null) {
            cookie = UUID.randomUUID().toString()
            val cookieOptions: CookieOptions = this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_OPTIONS_NAME)
            this.cookie(
                Cookie(
                    this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_NAME),
                    cookie,
                    cookieOptions.path,
                    cookieOptions.maxAge,
                    cookieOptions.secure,
                    cookieOptions.version,
                    cookieOptions.isHttpOnly,
                    cookieOptions.comment,
                    cookieOptions.domain,
                    cookieOptions.sameSite
                )
            )
        }

        val objectMapper = this.redisBsonObjectMapper()
        val byteBuffer = ByteBuffer.wrap(objectMapper.writeValueAsBytes(value))
        val cookieOptions: CookieOptions = this.appAttribute(RedisPlugin.ATTRIBUTE_COOKIE_OPTIONS_NAME)
        connection.async().setex(
            cookie,
            cookieOptions.maxAge.toLong(),
            byteBuffer
        ).toCompletableFuture()
            .thenApply { it == "Ok" }
    }
}
