package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderData {
    /**
     * * 订单数据格式如下类型字段说明
     *
     *   * 订单编号
     *   * 订单状态
     *     * 1.创建订单,等待支付
     *     * 2.支付订单完成
     *     * 3.取消订单，申请退款
     *     * 4.已发货
     *     * 5.确认收货，已经完成
     *   * 订单创建时间
     *   * 订单金额
     */
    //订单编号
    private String orderId;
    //订单状态
    private String status;
    //下单时间
    private String orderCreateTime;
    //订单金额
    private Double price;
}
