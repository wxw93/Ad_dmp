package cn.bupt.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.execution.columnar.NULL;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class GaoDeCeoApi {
    public static String  getBusness(String LongAndLat) throws Exception {

    //public void getBussness() throws Exception{
        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("output", "json");
        paramsMap.put("location", LongAndLat);
        paramsMap.put("key", "875f7bfd8fc776ff9f8fc93ca46a22d3");
        paramsMap.put("radius", "1000");
        paramsMap.put("extensions", "all");

        String paramsStr = toQueryString(paramsMap);
        String businessString = null;

        //相当于浏览器
        HttpClient httpClient = new HttpClient();
        GetMethod getMethod = new GetMethod("https://restapi.amap.com/v3/geocode/regeo?"+paramsStr);
        //返回访问浏览器状态码
        int code = httpClient.executeMethod(getMethod);
        if(code == 200){
            String responseBody = getMethod.getResponseBodyAsString();
            //关闭getMethod
            getMethod.releaseConnection();
            //解析json字符串 -- fastJson
            JSONObject jsonObject = JSON.parseObject(responseBody);
            //System.out.println(jsonObject);
            JSONObject regeoCode = jsonObject.getJSONObject("regeocode");
            JSONObject addressComponent = regeoCode.getJSONObject("addressComponent");
            JSONArray businessAreas = addressComponent.getJSONArray("businessAreas");
            //System.out.println(LongAndLat);
            if(businessAreas!= null && businessAreas.size()>1){
                JSONObject jsonArray = businessAreas.getJSONObject(0);
                businessString = jsonArray.getString("name");
                //System.out.println(name);
            }
        }
        return businessString;
    }

    // 对Map内所有value作utf8编码，拼接返回结果
    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }
}
