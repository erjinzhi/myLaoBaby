package com.bigdata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class NewsInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 获取数据本身
        String text = new String(event.getBody());
        // xxsueukwwer-xjkjkdkfjksdjkf
        String[] split = text.split("-");
        if(split.length == 2){
            //4. 获取到解码的字符串
            String meta = new String(Base64.decodeBase64(split[0]));
            String content = new String(Base64.decodeBase64(split[1]));
            //5、meta 的格式 {"project":"news","ip":"127.0.0.1","ctime":1589781236541}
            // content {
            //    "content":{
            //        "distinct_id":"51818968",
            //        "event":"AppClick",
            //        "properties":{
            //            "model":"",
            //            "network_type":"WIFI",
            //            "is_charging":"1",
            //            "app_version":"1.0",
            //            "element_name":"tag",
            //            "element_page":"新闻列表页",
            //            "carrier":"中国电信",
            //            "os":"GNU/Linux",
            //            "imei":"886113235750",
            //            "battery_level":"11",
            //            "screen_width":"640",
            //            "screen_height":"320",
            //            "device_id":"886113235750",
            //            "client_time":"2020-05-18 13:53:56",
            //            "ip":"61.233.88.41",
            //            "manufacturer":"Apple"
            //        }
            //    }
            //}
            JSONObject jsonObject = JSON.parseObject(meta);
            String ctime = jsonObject.getString("ctime");//72238423334
            // 72238423334  --> 20230630
            DateFormat fmt = new SimpleDateFormat("yyyyMMdd");
            ctime = fmt.format(Double.parseDouble(ctime)); // 20220622
            String projectName = jsonObject.getString("project");
            JSONObject contentObject = JSON.parseObject(content);
            String contentJosn = contentObject.getString("content");

            JSONObject jsonObjectVal = new JSONObject();
            jsonObjectVal.put("ctime",ctime);
            jsonObjectVal.put("project",projectName);
            jsonObjectVal.put("content",contentJosn);
            event.setBody(jsonObjectVal.toString().getBytes());

        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        List<Event> newList = new ArrayList<>();

        for (Event event:list) {
          Event newEvent =   intercept(event);
          newList.add(newEvent);
        }
        return newList;
    }

    @Override
    public void close() {

    }

    // 需要写一个内部类，用于new 这个新的interceptor
    public static class BuilderEvent implements Builder{

        @Override
        public Interceptor build() {
            return new NewsInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
