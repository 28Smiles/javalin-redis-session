package io.javalin.plugin.redis

import com.fasterxml.jackson.databind.ObjectMapper
import de.undercouch.bson4jackson.BsonFactory
import io.javalin.Javalin
import io.javalin.core.plugin.Plugin
import io.javalin.core.plugin.PluginLifecycleInit
import io.javalin.http.Cookie
import io.javalin.plugin.redis.codec.AdvancedCompressionCodec
import io.lettuce.core.AbstractRedisClient
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import java.nio.ByteBuffer

/**
 * @author Leon Camus
 * @since 08.05.2022
 */
class RedisPlugin(val options: RedisOptions) : Plugin, PluginLifecycleInit {
    override fun init(app: Javalin) {
        val clientConnection: Pair<AbstractRedisClient, StatefulConnection<String, ByteBuffer?>> = if (options.uri.size == 1) {
            val client = RedisClient.create(options.config, options.uri.first())

            Pair(client, client.connect(
                RedisCodec.of(
                    StringCodec(),
                    AdvancedCompressionCodec()
                )
            ))
        } else {
            if (options.uri.isEmpty()) {
                val client = RedisClient.create(options.config)

                Pair(client, client.connect(
                    RedisCodec.of(
                        StringCodec(),
                        AdvancedCompressionCodec()
                    )
                ))
            } else {
                val client = RedisClusterClient.create(options.config, options.uri)

                Pair(client, client.connect(
                    RedisCodec.of(
                        StringCodec(),
                        AdvancedCompressionCodec()
                    )
                ))
            }
        }
        val (client, connection) = clientConnection
        app.attribute(ATTRIBUTE_CONNECTION_NAME, connection)
        app.attribute(ATTRIBUTE_COOKIE_NAME, options.cookieName)
        app.attribute(ATTRIBUTE_MAP_NAME, options.mapName)
        app.attribute(ATTRIBUTE_COOKIE_OPTIONS_NAME, options.cookieOptions)
        app.attribute(ATTRIBUTE_OBJECT_MAPPER_NAME, options.objectMapper)

        app.events {
            it.serverStopping {
                connection.close()
                client.shutdown()
            }
        }
    }

    override fun apply(app: Javalin) {
        if (this.options.autoRefresh) {
            app.before { ctx ->
                val cookie = ctx.cookie(this.options.cookieName)
                if (cookie != null) {
                    // Refresh
                    val connection: StatefulRedisConnection<String, ByteBuffer?> = ctx.appAttribute(
                        ATTRIBUTE_CONNECTION_NAME
                    )
                    if (connection.sync().expire(cookie, this.options.cookieOptions.maxAge.toLong())) {
                        val cookieOptions: CookieOptions = ctx.appAttribute(ATTRIBUTE_COOKIE_OPTIONS_NAME)
                        ctx.cookie(
                            Cookie(
                                this.options.cookieName,
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
                }
            }
        }
    }

    companion object {
        const val ATTRIBUTE_CONNECTION_NAME = "RedisPluginStatefulConnection"
        const val ATTRIBUTE_COOKIE_NAME = "RedisPluginCookieName"
        const val ATTRIBUTE_COOKIE_OPTIONS_NAME = "RedisPluginCookieOptions"
        const val ATTRIBUTE_MAP_NAME = "RedisPluginAttributeMap"
        const val ATTRIBUTE_OBJECT_MAPPER_NAME = "RedisPluginObjectMapper"
    }
}
