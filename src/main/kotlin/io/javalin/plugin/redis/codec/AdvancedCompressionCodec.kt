package io.javalin.plugin.redis.codec

import com.nixxcode.jvmbrotli.common.BrotliLoader
import com.nixxcode.jvmbrotli.dec.BrotliInputStream
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream
import io.lettuce.core.codec.RedisCodec
import net.jpountz.lz4.LZ4BlockInputStream
import net.jpountz.lz4.LZ4BlockOutputStream
import net.jpountz.lz4.LZ4FrameInputStream
import net.jpountz.lz4.LZ4FrameOutputStream
import org.xerial.snappy.SnappyFramedInputStream
import org.xerial.snappy.SnappyFramedOutputStream
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
        if (BrotliLoader.isBrotliAvailable()) CompressionType.BROTLI else CompressionType.GZIP
    ),
    val minCompressionRatio: Long = 80_00L, // 80% of the original size
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

        val originalSize = value.remaining()
        val maximalSize: Long = originalSize * minCompressionRatio / 10_000
        val byteArray: ByteArray = if (originalSize >= minPayloadBytes) {
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
            if (acc.size <= bytes.size || bytes.size >= maximalSize) {
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
        val NONE: CompressionType = CompressionType(0x0, { it }, { it })
        val DEFLATE: CompressionType = CompressionType(0x1, ::DeflaterOutputStream, ::InflaterInputStream)
        val GZIP: CompressionType = CompressionType(0x2, ::GZIPOutputStream, ::GZIPInputStream)
        val BROTLI: CompressionType? = CompressionType(
            0x3,
            ::BrotliOutputStream,
            ::BrotliInputStream
        )
        val LZ4FRAME: CompressionType? = CompressionType(0x4, ::LZ4FrameOutputStream, ::LZ4FrameInputStream)
        val LZ4BLOCK: CompressionType? = CompressionType(0x5, ::LZ4BlockOutputStream, ::LZ4BlockInputStream)
        val SNAPPY: CompressionType? = CompressionType(0x6, ::SnappyFramedOutputStream, ::SnappyFramedInputStream)
    }
}