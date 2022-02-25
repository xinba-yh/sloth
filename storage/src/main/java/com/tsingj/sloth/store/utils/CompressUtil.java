package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.Result;
import com.tsingj.sloth.store.Results;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author yanghao
 */
public class CompressUtil {

    @Slf4j
    public static class GZIP {

        public static Result<byte[]> compress(byte[] str) {
            if (str == null || str.length == 0) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                gzip.write(str);
            } catch (IOException e) {
                log.error("GZIP compress fail!", e);
                return Results.failure("GZIP compress fail! " + e.getMessage());
            }
            return Results.success(out.toByteArray());
        }


        public static Result<byte[]> uncompress(byte[] compressed) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ByteArrayInputStream in = new ByteArrayInputStream(compressed);
                 GZIPInputStream ginzip = new GZIPInputStream(in)) {
                byte[] buffer = new byte[1024];
                int offset;
                while ((offset = ginzip.read(buffer)) != -1) {
                    out.write(buffer, 0, offset);
                }
                return Results.success(out.toByteArray());
            } catch (IOException e) {
                log.error("compress fail!", e);
                return Results.failure("compress fail!" + e.getMessage());
            }

        }

    }


}
