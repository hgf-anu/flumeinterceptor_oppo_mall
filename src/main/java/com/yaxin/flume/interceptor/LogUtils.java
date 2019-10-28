/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: LogUtils
 * Author: hgf
 * Date: 2019/10/27 0027 上午 2:28
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名修改时间版本号描述
 */
package com.yaxin.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 〈一句话功能简述〉<br>
 * 进行简单的ETL操作。对数据的长度、数据格式是否符合要求等等
 *
 * @author hgf
 * @create 2019/10/27 0027
 * @since 1.0.0
 */
public class LogUtils{

	public static boolean valuateStart( String log ){
		//log就是json串，为NULL就返回false,防止出现空指针异常
		if( log == null ){
			return false;
		}
		//开头不是“｛”或者不是“}”结尾就返回false，判断数据的完整性
		if( !log.trim().startsWith("{") || !log.trim().endsWith("}") ){
			return false;
		}
		return true;
	}

	public static boolean valuateEvent( String log ){
		if( log == null ){
			return false;
		}
		//数据结构是：时间|json，需要先切割出json串
		//1.切割
		String[] logContents = log.split("\\|");

		//2.校验
		if( logContents.length != 2 ){
			return false;
		}

		//3.先判断服务器时间
		// 判断条件：（1）服务器时间的尾数是13位；（2）全是数字
		if( logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0]) ){
			return false;
		}
		//4.判断JSON，大括号开头，大括号结尾
		if( !logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}") ){
			return false;
		}
		return true;
	}
}
