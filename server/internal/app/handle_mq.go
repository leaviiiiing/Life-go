package app

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
)

func (a *App) getMQFailedLogs(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}
	ctx := c.Request.Context()
	var rows []MqKafkaLog
	_ = a.DB.SelectContext(ctx, &rows, `SELECT id,msg_id,biz_type,biz_key,topic,partition_id,offset_val,direction,status,error_msg,create_time FROM tb_mq_kafka_log WHERE status='FAILED' ORDER BY create_time DESC LIMIT ?`, limit)
	c.JSON(http.StatusOK, dto.OkData(rows))
}

func (a *App) postMQVoucherRepublish(c *gin.Context) {
	var body VoucherOrderRepublishRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusOK, dto.Fail("body 不能为空"))
		return
	}
	if body.OrderID == 0 || body.UserID == 0 || body.VoucherID == 0 {
		c.JSON(http.StatusOK, dto.Fail("orderId、userId、voucherId 不能为空"))
		return
	}
	msg, err := a.republishVoucherOrder(c.Request.Context(), body.OrderID, body.UserID, body.VoucherID)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, dto.OkData(msg))
}
