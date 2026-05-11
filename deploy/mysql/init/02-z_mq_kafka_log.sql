-- Kafka 消息可靠性落库（在已有库 hmdp 上增量执行；新环境若仅执行本文件请先 USE hmdp;）
USE hmdp;

CREATE TABLE IF NOT EXISTS tb_mq_kafka_log (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
  msg_id VARCHAR(64) NOT NULL COMMENT '消息唯一标识，与 Kafka 头 MSG_ID 一致',
  biz_type VARCHAR(32) DEFAULT NULL COMMENT '业务类型，如 VOUCHER_ORDER',
  biz_key VARCHAR(128) DEFAULT NULL COMMENT '业务键，如 orderId',
  topic VARCHAR(128) DEFAULT NULL COMMENT 'Topic',
  partition_id INT DEFAULT NULL COMMENT '分区',
  offset_val BIGINT DEFAULT NULL COMMENT '位点',
  direction VARCHAR(16) NOT NULL COMMENT 'SEND CONSUME DLT',
  status VARCHAR(16) NOT NULL COMMENT 'SUCCESS FAILED',
  error_msg VARCHAR(1024) DEFAULT NULL COMMENT '失败原因',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_msg_id (msg_id),
  KEY idx_topic_offset (topic, partition_id, offset_val)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka 发送/消费/死信审计';
