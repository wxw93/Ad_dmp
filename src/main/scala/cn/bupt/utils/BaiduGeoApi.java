package cn.bupt.utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class BaiduGeoApi {

    public static String getBussine(String LatAndLong) throws Exception {
//    @Test
//    public void  getBussine() throws Exception {
        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("ak", "p2Eyx6hkWKkrGYNEQOzLPUimg7qSTVzt");
        paramsMap.put("output", "json");
        paramsMap.put("coordtype", "wgs84ll");
        paramsMap.put("location", "31.225696563611,121.49884033194");

        String businessString = null;

        String paramsStr = toQueryString(paramsMap);

        String wholeStr = new String("/geocoder/v2/?" + paramsStr + "6ngjkLtctGXGoZP8lhv9wYKCPdx9kQQi");
        System.out.println(wholeStr);
        // 对上面wholeStr再作utf8编码
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");

        // 调用下面的MD5方法得到最后的sn签名
        System.out.println(MD5(tempStr));

        //相当于浏览器
        HttpClient httpClient = new HttpClient();
        GetMethod getMethod = new GetMethod("http://api.map.baidu.com/geocoder/v2/?"+paramsStr + "&sn="+MD5(tempStr));
        //返回访问浏览器状态码
        int code = httpClient.executeMethod(getMethod);
        if(code == 200){
            String responseBody = getMethod.getResponseBodyAsString();
            //关闭getMethod
            getMethod.releaseConnection();
            System.out.println(responseBody);
            String replaced = responseBody.replace("renderReverse&&renderReverse(", "");
            //System.out.println(replaced);
            String substring = replaced.substring(0, replaced.lastIndexOf(")"));

            //解析json字符串 -- fastJson
            JSONObject jsonObject = JSON.parseObject(substring);
            JSONObject resultJson = jsonObject.getJSONObject("result");
            businessString = resultJson.getString("business");
            //System.out.println(businessString);
            if(StringUtils.isEmpty(businessString)){
                JSONArray poisArray = resultJson.getJSONArray("pois");
                if(poisArray != null && poisArray.size()>0){
                    businessString = poisArray.getJSONObject(0).getString("tag");
                }
            }
            System.out.println(businessString);
        }
        return businessString;
    }

    // 对Map内所有value作utf8编码，拼接返回结果
    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }
}
