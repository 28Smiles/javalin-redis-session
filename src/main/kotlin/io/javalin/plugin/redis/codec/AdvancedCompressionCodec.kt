package io.javalin.plugin.redis.codec

import com.nixxcode.jvmbrotli.common.BrotliLoader
import com.nixxcode.jvmbrotli.dec.BrotliInputStream
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream
import io.lettuce.core.codec.RedisCodec
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.InflaterInputStream

/**
 * @author Leon Camus
 * @since 11.05.2022
 */
class AdvancedCompressionCodec(
    val compressions: List<CompressionType> = listOfNotNull(
        CompressionType.NONE,
        CompressionType.DEFLATE,
        CompressionType.GZIP,
        if (BrotliLoader.isBrotliAvailable()) CompressionType.BROTLI else null
    ),
    val minCompressionRatio: Float = 0.8F, // 80% of the original size
    val minPayloadBytes: Long = 128L, // 128B
) : RedisCodec<ByteBuffer, ByteBuffer?> {
    init {
        if (compressions.isEmpty()) {
            throw IllegalStateException()
        }
    }

    override fun encodeKey(key: ByteBuffer?): ByteBuffer = key ?: ByteBuffer.allocate(0)

    override fun decodeKey(bytes: ByteBuffer?): ByteBuffer = bytes ?: ByteBuffer.allocate(0)

    override fun encodeValue(value: ByteBuffer?): ByteBuffer? {
        if (value == null) {
            return null
        }

        if (value.remaining() == 0) {
            return value
        }

        val byteArray: ByteArray = if (value.remaining() >= minPayloadBytes) {
            this.compressions
        } else {
            listOf(CompressionType.NONE)
        }.map { compressionType ->
            val outputStream = ByteArrayOutputStream(value.remaining())
            outputStream.write(compressionType.indicator.toInt())
            val compressionStream = compressionType.outputStream(outputStream)
            compressionStream.write(value.array())
            compressionStream.close()
            val byteArray = outputStream.toByteArray()
            outputStream.close()
            byteArray
        }.reduce { acc, bytes ->
            if (acc.size <= bytes.size) {
                acc
            } else {
                bytes
            }
        }

        return ByteBuffer.wrap(byteArray)
    }

    override fun decodeValue(bytes: ByteBuffer?): ByteBuffer? {
        if (bytes == null) {
            return null
        }

        if (bytes.remaining() == 0) {
            return bytes
        }

        val sourceStream = ByteArrayInputStream(if (bytes.hasArray()) {
            bytes.array()
        } else {
            val array = ByteArray(bytes.remaining())

            for (i in array.indices) {
                array[i] = bytes.get()
            }

            array
        })
        val indicator = sourceStream.read().toByte()
        val compression = this.compressions.firstOrNull { it.indicator == indicator } ?: throw IllegalStateException()
        val decompressionStream = compression.inputStream(sourceStream)
        val decompressedArray = decompressionStream.readBytes()
        sourceStream.close()
        decompressionStream.close()

        return ByteBuffer.wrap(decompressedArray)
    }
}

class CompressionType(
    val indicator: Byte,
    val outputStream: (OutputStream) -> OutputStream,
    val inputStream: (InputStream) -> InputStream
) {
    companion object {
        val NONE = CompressionType(0x0, { it }, { it })
        val DEFLATE = CompressionType(0x1, ::DeflaterOutputStream, ::InflaterInputStream)
        val GZIP = CompressionType(0x2, ::GZIPOutputStream, ::GZIPInputStream)
        val BROTLI = CompressionType(0x3, ::BrotliOutputStream, ::BrotliInputStream)
    }
}