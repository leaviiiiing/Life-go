package app

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
)

func (a *App) postVoucher(c *gin.Context) {
	var v struct {
		ShopID      int64  `json:"shopId"`
		Title       string `json:"title"`
		SubTitle    string `json:"subTitle"`
		Rules       string `json:"rules"`
		PayValue    int64  `json:"payValue"`
		ActualValue int64  `json:"actualValue"`
		Type        int    `json:"type"`
	}
	if err := c.ShouldBindJSON(&v); err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	ctx := c.Request.Context()
	res, err := a.DB.ExecContext(ctx, `INSERT INTO tb_voucher (shop_id,title,sub_title,rules,pay_value,actual_value,type,status) VALUES (?,?,?,?,?,?,?,1)`,
		v.ShopID, v.Title, v.SubTitle, v.Rules, v.PayValue, v.ActualValue, v.Type)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("保存失败"))
		return
	}
	id, _ := res.LastInsertId()
	c.JSON(http.StatusOK, dto.OkData(id))
}

func (a *App) postVoucherSeckill(c *gin.Context) {
	var v struct {
		ShopID      int64     `json:"shopId"`
		Title       string    `json:"title"`
		SubTitle    string    `json:"subTitle"`
		Rules       string    `json:"rules"`
		PayValue    int64     `json:"payValue"`
		ActualValue int64     `json:"actualValue"`
		Type        int       `json:"type"`
		Stock       int       `json:"stock"`
		BeginTime   time.Time `json:"beginTime"`
		EndTime     time.Time `json:"endTime"`
	}
	if err := c.ShouldBindJSON(&v); err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	ctx := c.Request.Context()
	tx, err := a.DB.BeginTxx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("事务失败"))
		return
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO tb_voucher (shop_id,title,sub_title,rules,pay_value,actual_value,type,status) VALUES (?,?,?,?,?,?,?,1)`,
		v.ShopID, v.Title, v.SubTitle, v.Rules, v.PayValue, v.ActualValue, v.Type)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("保存失败"))
		return
	}
	id, _ := res.LastInsertId()
	_, err = tx.ExecContext(ctx, `INSERT INTO tb_seckill_voucher (voucher_id,stock,begin_time,end_time) VALUES (?,?,?,?)`, id, v.Stock, v.BeginTime, v.EndTime)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("保存秒杀失败"))
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusOK, dto.Fail("提交失败"))
		return
	}
	_ = a.RDB.Set(ctx, "seckill:stock:"+strconv.FormatInt(id, 10), strconv.Itoa(v.Stock), 0).Err()
	c.JSON(http.StatusOK, dto.OkData(id))
}

func (a *App) getVoucherList(c *gin.Context) {
	sid, _ := strconv.ParseInt(c.Param("shopId"), 10, 64)
	ctx := c.Request.Context()
	var list []Voucher
	_ = a.DB.SelectContext(ctx, &list, `SELECT v.id, v.shop_id, v.title, v.sub_title, v.rules, v.pay_value, v.actual_value, v.type, sv.stock, sv.begin_time, sv.end_time
		FROM tb_voucher v LEFT JOIN tb_seckill_voucher sv ON v.id = sv.voucher_id WHERE v.shop_id = ? AND v.status = 1`, sid)
	c.JSON(http.StatusOK, dto.OkData(list))
}
