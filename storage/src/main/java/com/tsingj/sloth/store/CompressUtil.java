package com.tsingj.sloth.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author yanghao
 */
public class CompressUtil {

    public static class GZIP {
        /**
         * 使用gzip压缩字符串
         *
         * @param str 要压缩的字符串
         * @return
         */
//        public static String compress(String str) {
//            if (str == null || str.length() == 0) {
//                return str;
//            }
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
//                gzip.write(str.getBytes());
//            } catch (IOException ignored) {
//                return null;
//            }
//            return new sun.misc.BASE64Encoder().encode(out.toByteArray());
//        }

        /**
         * 使用gzip压缩字符串
         *
         * @param str 要压缩的字节数组
         * @return
         */
        public static String compressWithBytes(byte[] str) {
            if (str == null || str.length == 0) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                gzip.write(str);
            } catch (IOException ignored) {
                return null;
            }
            return Base64.getEncoder().encodeToString(out.toByteArray());
        }

        /**
         * 使用gzip解压缩
         *
         * @param compressedStr 压缩字符串
         * @return
         */
//        public static String uncompress(String compressedStr) {
//            if (compressedStr == null) {
//                return null;
//            }
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            ByteArrayInputStream in = null;
//            GZIPInputStream ginzip = null;
//            byte[] compressed;
//            String decompressed;
//            try {
//                compressed = new sun.misc.BASE64Decoder().decodeBuffer(compressedStr);
//                in = new ByteArrayInputStream(compressed);
//                ginzip = new GZIPInputStream(in);
//                byte[] buffer = new byte[1024];
//                int offset;
//                while ((offset = ginzip.read(buffer)) != -1) {
//                    out.write(buffer, 0, offset);
//                }
//                decompressed = out.toString();
//            } catch (IOException ignored) {
//                return null;
//            } finally {
//                releaseGzip(ginzip, in, out);
//            }
//            return decompressed;
//        }


        public static byte[] uncompressRespBytes(String compressedStr) {
            if (compressedStr == null) {
                return null;
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = null;
            GZIPInputStream ginzip = null;
            byte[] compressed;
            byte[] decompressed;
            try {
                compressed = Base64.getDecoder().decode(compressedStr);
                in = new ByteArrayInputStream(compressed);
                ginzip = new GZIPInputStream(in);
                byte[] buffer = new byte[1024];
                int offset;
                while ((offset = ginzip.read(buffer)) != -1) {
                    out.write(buffer, 0, offset);
                }
                decompressed = out.toByteArray();
            } catch (IOException ignored) {
                return null;
            } finally {
                releaseGzip(ginzip, in, out);
            }
            return decompressed;
        }

        private static void releaseGzip(GZIPInputStream ginzip, ByteArrayInputStream in, ByteArrayOutputStream out) {
            if (ginzip != null) {
                try {
                    ginzip.close();
                } catch (IOException ignored) {
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
            try {
                out.close();
            } catch (IOException ignored) {
            }
        }

    }

    public static void main(String[] args) throws IOException {
        String s = "{'code': 'entry_reduce1_func.py\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x000000644\\x000000765\\x000000024\\x0000000000213\\x0013707743133\\x00016553\\x00 0\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00ustar  \\x00xieyunying\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00staff\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x000000000\\x000000000\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00import privpy as pp\\nnum = pp.ss(\"plus1\")\\npp.debug_reveal(num)\\nnum = num + pp.sint(1)\\npp.reveal(num, \"cipher://privpy-v112-c2/gold/tableA\")\\n\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00', 'description': 'GeminiTask', 'name': 'test_demo-20200728143155_job0_reduce1_func_0_test_demo-20200728143155', 'src': 'web', 'taskDataSourceVOList': [{'dataUrl': 'cipher://privpy-v112-c2/gold/tableA', 'varName': 'plus1'}], 'taskResultVOList': [{'resultDest': 'privpy-v112-c2', 'resultVarName': 'cipher://privpy-v112-c2/gold/tableA'}], 'taskEntrypoint': 'entry_reduce1_func.py'}";
        System.out.println("字符串长度：" + s.length());
//        String o1 = CompressUtil.GZIP.compress(s);
//        System.out.println("压缩后：：" + o1.length());
//        String ouncompress = CompressUtil.GZIP.uncompress(o1);
//        System.out.println("解压后：" + ouncompress.length());
//        System.out.println(ouncompress.equals(s));
    }

}
