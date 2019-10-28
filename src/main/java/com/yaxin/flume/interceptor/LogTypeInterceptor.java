/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: LogTypeInterceptor
 * Author: hgf
 * Date: 2019/10/27 0027 下午 14:48
 * Description: Type
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名修改时间版本号描述
 */
package com.yaxin.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 〈一句话功能简述〉<br>
 * 〈Type〉
 *
 * @author hgf
 * @create 2019/10/27 0027
 * @since 1.0.0
 */
public class LogTypeInterceptor implements Interceptor{

	@Override
	public void initialize(){

	}

	@Override
	public Event intercept( Event event ){
		//目标：json-> start、event放到header，在header中标记
		//1.获取body数据
		byte[] body = event.getBody();
		String log = new String(body,Charset.forName("UTF-8"));

		//2.获取header
		Map< String, String > headers = event.getHeaders();
		//添加key和value

		//3.向header里添加值
		if( log.contains("start") ){
			//将数据标记为不同topic名字.比如有12张表，可能一张表一个topic
			headers.put("topic","topic_start");
		}else{
			headers.put("topic","topic_event");
		}
		return event;
	}

	@Override
	public List< Event > intercept( List< Event > events ){
		//进一步解耦，使用额外的集合，防止源数据被修改
		ArrayList< Event > interceptors = new ArrayList<>();

		for( Event event : events ){
			Event intercept1 = intercept(event);

			interceptors.add(intercept1);
		}
		return interceptors;
	}

	@Override
	public void close(){

	}

	public static class Builder implements Interceptor.Builder{

		@Override
		public Interceptor build(){
			return new LogTypeInterceptor();
		}

		@Override
		public void configure( Context context ){

		}
	}
}
