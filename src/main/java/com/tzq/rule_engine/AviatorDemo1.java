package com.tzq.rule_engine;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title 示例1： 设计一个根据付款金额决定是否发送优惠券的规则表达式
 * @Author zhengqiang.tan
 * @Date 3/20/23 11:25 AM
 */
public class AviatorDemo1 {
    /**
     * 自定义当用户买的商品付款金额大于A价格的时候就认为他啊可以参与抽奖。
     */
    public static void main(String[] args) {
        AviatorEvaluator.addFunction(new IsDiscountFunction());

        Map<String, Object> map = new HashMap(4);
        map.put("discount", new BigDecimal(0.1));
        map.put("price", new BigDecimal(20.0));
        map.put("limit", new BigDecimal(100.00));

        //编译且执行表达式。
        Object isDiscount = AviatorEvaluator.execute("isDiscount(discount,price,limit)", map);
        System.out.println(isDiscount); // true
    }

    /**
     * 自定义函数 是否可以拿到折扣
     * 在这里得精度计算
     */
    static class IsDiscountFunction extends AbstractFunction {
        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3) {
            //arg和对应的map中的value对应
            BigDecimal price = (BigDecimal) FunctionUtils.getNumberValue(arg1, env);
            BigDecimal discount = (BigDecimal) FunctionUtils.getNumberValue(arg2, env);
            BigDecimal limit = (BigDecimal) FunctionUtils.getNumberValue(arg3, env);
            //逻辑
            BigDecimal paymentAmount = price.multiply(discount); // 20 x 0.1 = 2
//            int gap = paymentAmount.compareTo(limit); // 2 和 100 比 肯定是 -1
            return AviatorBoolean.valueOf(paymentAmount.compareTo(limit) > -1 ? false : true);
        }

        @Override
        public String getName() {
            return "isDiscount"; // 用来生成代码的时候的函数名
        }
    }
}
