package io.javalin.plugin.redis

import com.fasterxml.jackson.databind.ObjectMapper
import de.undercouch.bson4jackson.BsonFactory
import io.javalin.http.SameSite
import io.lettuce.core.RedisURI
import io.lettuce.core.resource.ClientResources

data class CookieOptions(
    var path: String = "/",
    var maxAge: Int = 60 * 60 * 6,
    var secure: Boolean = false,
    var version: Int = 0,
    var isHttpOnly: Boolean = false,
    var comment: String? = null,
    var domain: String? = null,
    var sameSite: SameSite? = null
)

class RedisOptions {
    val uri: MutableSet<RedisURI> = mutableSetOf()
    var config: ClientResources = ClientResources.create()
    var objectMapper: ObjectMapper = ObjectMapper(BsonFactory())
    var mapName: String = "javalin-session"
    var cookieName: String = "session"
    var cookieOptions: CookieOptions = CookieOptions()
    var autoRefresh: Boolean = true

    fun poolConfig(poolConfig: ClientResources) = apply { this.config = poolConfig }
    fun uri(uri: RedisURI) = apply { this.uri.add(uri) }
}
