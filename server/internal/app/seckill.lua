-- 1. 参数列表
-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
--订单id
local orderId = ARGV[3]

-- 2. 数据key
-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3. 脚本业务
-- 判断库存是否充足 get stockKey
if(tonumber(redis.call('get', stockKey)) <= 0) then
    -- 库存不足，返回1
    return 1
end
-- 判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    --存在，说明已下过单，返回2
    return 2
end
--扣库存incrby stockKey -1
redis.call('incrby',stockKey,-1)
--下单（保存用户）sadd orderKey userId
redis.call('sadd',orderKey,userId)
--发送消息到队列
return 0
