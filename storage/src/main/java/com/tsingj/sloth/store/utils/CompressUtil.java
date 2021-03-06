package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author yanghao
 */
public class CompressUtil {

    public static class GZIP {

        private static final Logger logger = LoggerFactory.getLogger(CompressUtil.class);

        public static Result<byte[]> compress(byte[] str) {
            if (str == null || str.length == 0) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                gzip.write(str);
            } catch (IOException e) {
                logger.error("GZIP compress fail!", e);
                return Results.failure("GZIP compress fail! " + e.getMessage());
            }
            return Results.success(out.toByteArray());
        }


        public static Result<byte[]> uncompress(byte[] compressed) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ByteArrayInputStream in = new ByteArrayInputStream(compressed);
                 GZIPInputStream gzip = new GZIPInputStream(in)) {
                byte[] buffer = new byte[1024];
                int offset;
                while ((offset = gzip.read(buffer)) != -1) {
                    out.write(buffer, 0, offset);
                }
                return Results.success(out.toByteArray());
            } catch (IOException e) {
                logger.error("compress fail!", e);
                return Results.failure("compress fail!" + e.getMessage());
            }
        }

    }


}
