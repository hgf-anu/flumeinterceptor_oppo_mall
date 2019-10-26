/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: LogETLInterceptor
 * Author: hgf
 * Date: 2019/10/27 0027 上午 2:18
 * Description: ETL
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

/**
 * 〈一句话功能简述〉<br>
 * 〈ETL〉
 *
 * @author hgf
 * @create 2019/10/27 0027
 * @since 1.0.0
 */
public class LogETLInterceptor implements Interceptor{

	/**
	* 初始化
	*/
	@Override
	public void initialize(){

	}

	/**
	 * 单event处理，进行简单ETL，处理JSON数据
	 * 启动日志【单JSON】和事件日志【服务器时间|JSON】不同
	 */
	@Override
	public Event intercept( Event event ){
		//1.先获取数据
		byte[] body = event.getBody();
		String log = new String(body,Charset.forName("UTF-8"));
		//2.校验：启动日志和事件日志
		//其他信息一定要回避“start”【可以自己定义一个特定的符号代表启动日志】
		if(log.contains("start")){
			//注意：这里不要写大量的逻辑代码，抽取出工具类、方法的形式
			//2.1校验启动日志
			if( LogUtils.valuateStart(log) ){
				return event;
			}
		}else {
			if(LogUtils.valuateEvent(log)){
				return event;
			}
		}
		//前面不符合条件的这里都返回NULL
		return null;
	}

	/**
	 * event集合处理,过滤得到符合要求的event
	 */
	@Override
	public List< Event > intercept( List< Event > events ){
		//该集合存储正确的event
		ArrayList< Event > interceptors = new ArrayList<>();

		//对events进行遍历，每一个都使用单event进行验证
		for( Event event : events ){
			Event intercept = intercept(event);
			//返回true的event放到一个集合里作为返回值
			if(intercept!=null){
				interceptors.add(event);
			}
		}
		return interceptors;
	}

	/**
	 * 关闭
	 */
	@Override
	public void close(){

	}

	/**
	* 构建静态内部类，方便类的生成[new]
	*/
	public static class Builder implements Interceptor.Builder{

		@Override
		public Interceptor build(){
			return new LogETLInterceptor();
		}

		@Override
		public void configure( Context context ){

		}
	}
}
