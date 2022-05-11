package io.javalin.plugin.redis

import io.javalin.Javalin
import io.javalin.testtools.TestUtil
import io.lettuce.core.RedisURI
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID

data class TestCookie(val uuid: UUID = UUID.randomUUID())

/**
 * @author Leon Camus
 * @since 11.05.2022
 */
class ContextExtensionsTest {
    @Test
    fun testRedisSync() {
        val app = Javalin.create {
            it.registerPlugin(RedisPlugin(RedisOptions().uri(
                RedisURI("localhost", 6379, Duration.ofSeconds(2))
            )))
        }.exception(java.lang.Exception::class.java) { error, _ ->
            error.printStackTrace()
        }

        val refCookie = TestCookie()
        app.get("login") { ctx ->
            ctx.session(refCookie)
        }
        var testCookie = TestCookie()
        app.get("logout") { ctx ->
            testCookie = ctx.session()!!
        }

        TestUtil.test(app) { _, client ->
            val response = client.get("/login")
            assertThat(response.code).isEqualTo(200)
            val setCookie = response.headers["Set-Cookie"]!!
            val cookie = setCookie.split(";")[0]

            val logoutResponse = client.get("/logout") {
                it.addHeader("Cookie", cookie)
            }
            assertThat(logoutResponse.code).isEqualTo(200)
        }

        Assertions.assertEquals(refCookie, testCookie)
    }

    @Test
    fun testRedisAsync() {
        val app = Javalin.create {
            it.registerPlugin(RedisPlugin(RedisOptions().uri(
                RedisURI("localhost", 6379, Duration.ofSeconds(2))
            )))
        }.exception(java.lang.Exception::class.java) { error, _ ->
            error.printStackTrace()
        }

        val refCookie = TestCookie()
        app.get("login") { ctx ->
            ctx.future(ctx.sessionAsync(refCookie))
        }
        var testCookie = TestCookie()
        app.get("logout") { ctx ->
            ctx.future(ctx.sessionAsync<TestCookie>().thenApply {
                testCookie = it!!

                true
            })
        }

        TestUtil.test(app) { _, client ->
            val response = client.get("/login")
            assertThat(response.code).isEqualTo(200)
            val setCookie = response.headers["Set-Cookie"]!!
            val cookie = setCookie.split(";")[0]

            val logoutResponse = client.get("/logout") {
                it.addHeader("Cookie", cookie)
            }
            assertThat(logoutResponse.code).isEqualTo(200)
        }

        Assertions.assertEquals(refCookie, testCookie)
    }
}