package app

import (
	"github.com/gin-gonic/gin"

	"github.com/leaviiiiing/Life-go/server/internal/middleware"
)

func (a *App) RegisterMain(r *gin.Engine) {
	r.Use(middleware.TokenRefresh(a.RDB))
	r.Use(middleware.LoginRequired())

	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })

	u := r.Group("/user")
	{
		u.POST("/code", a.postUserCode)
		u.POST("/login", a.postUserLogin)
		u.POST("/logout", a.postUserLogout)
		u.GET("/me", a.getUserMe)
		u.GET("/info/:id", a.getUserInfo)
		u.GET("/sign/count", a.getUserSignCount)
		u.POST("/sign", a.postUserSign)
		u.GET("/:id", a.getUserByID)
	}

	r.GET("/shop-type/list", a.getShopTypeList)
	sh := r.Group("/shop")
	{
		sh.GET("/of/type", a.getShopOfType)
		sh.GET("/of/name", a.getShopOfName)
		sh.GET("/:id", a.getShopByID)
		sh.POST("", a.postShop)
		sh.PUT("", a.putShop)
	}

	bg := r.Group("/blog")
	{
		bg.POST("", a.postBlog)
		bg.PUT("/like/:id", a.putBlogLike)
		bg.GET("/of/me", a.getBlogOfMe)
		bg.GET("/hot", a.getBlogHot)
		bg.GET("/likes/:id", a.getBlogLikes)
		bg.GET("/of/user", a.getBlogOfUser)
		bg.GET("/of/follow", a.getBlogOfFollow)
		bg.GET("/:id", a.getBlogByID)
	}

	fo := r.Group("/follow")
	{
		fo.PUT("/:id/:isFollow", a.putFollow)
		fo.GET("/or/not/:id", a.getFollowOrNot)
		fo.GET("/common/:id", a.getFollowCommon)
	}

	r.POST("/upload/blog", a.postUploadBlog)
	r.GET("/upload/blog/delete", a.getUploadBlogDelete)

	v := r.Group("/voucher")
	{
		v.POST("", a.postVoucher)
		v.POST("/seckill", a.postVoucherSeckill)
		v.GET("/list/:shopId", a.getVoucherList)
	}

	vo := r.Group("/voucher-order")
	{
		vo.POST("/seckill/:id", a.postVoucherOrderSeckill)
		vo.POST("/pay/:orderId", a.postVoucherOrderPay)
	}

	mq := r.Group("/mq/compensation/kafka")
	{
		mq.GET("/failed-logs", a.getMQFailedLogs)
		mq.POST("/voucher/republish", a.postMQVoucherRepublish)
	}
}
