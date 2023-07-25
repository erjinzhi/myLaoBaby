package com.bigdata;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class DemoInterceptor implements Interceptor {
        /**
         * 初始化方法，当拦截器初始化的时候调用一次
         */
        public void initialize() {}

        /**
         * 处理单条数据
         * @param event 一条数据
         *
         * jksflkdsfjklasdjlfkas-fjaskfasdfjkasdflasdfljkasdlk
         */
        public Event intercept(Event event) {
            //1. 获取数据本身
            String text = new String(event.getBody());
            //2. 切割
            String[] textArray = text.split("-");

            byte[] body = null;
            //3. 判断
            if (textArray.length == 2) {
                try {
                    //4. 获取到解码的字符串
                    String meta = new String(Base64.decodeBase64(textArray[0]));
                    String content = new String(Base64.decodeBase64(textArray[1]));

                    //5. 将json的字符串转换为json的对象:ctime、project、ip
                    JSONObject jsonMeta = JSONObject.parseObject(meta);

                    //6. 获取到字段: ctime: 111111.111
                    String ctime = JSONPath.eval(jsonMeta, "$.ctime").toString();
                    DateFormat fmt = new SimpleDateFormat("yyyyMMdd");
                    ctime = fmt.format(Double.parseDouble(ctime)); // 20220622

                    //7. 将ctime的字段插入到flume的event的header中
                    event.getHeaders().put("ctime", ctime);

                    //8. 解析content
                    JSONObject jsonContent = JSONObject.parseObject(content);

                    //9. 将jsonContent和jsonMeta对象合并为一个json对象
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("ctime", JSONPath.eval(jsonMeta, "$.ctime"));
                    jsonObject.put("project", JSONPath.eval(jsonMeta, "$.project"));
                    jsonObject.put("content",JSONPath.eval(jsonContent, "$.content"));

                    //10. 复制body数组
                    body = jsonObject.toString().getBytes();
                }catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
            //11. 设置event的值
            event.setBody(body);
            return event;
        }

        /**
         * 自动被调用
         */
        public List<Event> intercept(List<Event> list) {
            //1. 创建数组返回这个结果
            ArrayList<Event> inter = Lists.newArrayListWithCapacity(list.size());
            //2. 遍历
            for(Event event : list) {
                Event e = intercept(event);
                if (e != null) inter.add(e);
            }
            return inter;
        }

        /**
         * 临死之前调用一次
         */
        public void close() {}

        /**
         * 申明Builder，这个方法会在flume拦截器创建的时候自动被调用
         */
        public static class Builder implements Interceptor.Builder {
            public Interceptor build() {
                return new MyInterceptor();
            }
            public void configure(Context context) {}
        }

    }
