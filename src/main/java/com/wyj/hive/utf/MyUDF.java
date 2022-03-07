package com.wyj.hive.utf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/*
 * 继承udf
 *
 * 给定一个基本数据类型
 * 返回一个基本数据类型的长度
 *
 * 后续需要写自定义函数:
 * 可以通过看GenericUDF的实现类(如GenericUDFOctetLength)
 * 通过模仿其实现类实现逻辑来写自己的自定义函数
 */
public class MyUDF extends GenericUDF {

    /**
     * 初始化方法:函数参数判断
     * 1.判断参数个数
     * 2.判断参数的类型
     * 3.约定返回值类型
     *
     * @param arguments 传入的参数
     *                  ObjectInspector为接口,里面有静态枚举类型Category
     *                  PRIMITIVE,LIST, MAP, STRUCT,UNION;
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //1.判断参数的个数
        if (arguments.length != 1) {
            //抛出UDF参数个数异常
            //能往下执行的函数保证其参数只能是一个
            throw new UDFArgumentLengthException("please give me only one arg");
        }

        //2.判断参数的类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            //抛出UDF参数类型异常
            //第一个参数异常
            throw new UDFArgumentTypeException(1, "only PRIMITIVE type");
        }

        //3.约定返回值类型
        //直接通过工厂类返回int类型的
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 实现业务逻辑
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if (o == null) {
            return 0;
        }
        return o.toString().length();
    }

    @Override
    //获取解释的字符串(啥用没有)
    //如果将来函数跑在MR中,能在跑MR的过程中打印字符串表示正在使用该函数
    public String getDisplayString(String[] children) {
        return null;
    }
}
