package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
)

const (
	cacheShopKey   = "cache:shop:"
	cacheNullTTL   = 2 * time.Minute
	cacheShopTTL   = 30 * time.Minute
	shopGeoKeyPref = "shop:geo:"
)

func (a *App) getShopByID(c *gin.Context) {
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if id <= 0 {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	ctx := c.Request.Context()
	key := cacheShopKey + c.Param("id")
	raw, err := a.RDB.Get(ctx, key).Result()
	if err == nil {
		if raw == "null" {
			c.JSON(http.StatusOK, dto.Fail("店铺不存在!"))
			return
		}
		if raw != "" {
			var sh Shop
			if json.Unmarshal([]byte(raw), &sh) == nil {
				c.JSON(http.StatusOK, dto.OkData(sh))
				return
			}
		}
	} else if err != redis.Nil {
		c.JSON(http.StatusOK, dto.Fail("服务异常"))
		return
	}
	var sh Shop
	e2 := a.DB.GetContext(ctx, &sh, `SELECT id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time FROM tb_shop WHERE id=?`, id)
	if e2 != nil {
		_ = a.RDB.Set(ctx, key, "null", cacheNullTTL).Err()
		c.JSON(http.StatusOK, dto.Fail("店铺不存在!"))
		return
	}
	b, _ := json.Marshal(sh)
	_ = a.RDB.Set(ctx, key, string(b), cacheShopTTL).Err()
	c.JSON(http.StatusOK, dto.OkData(sh))
}

func (a *App) postShop(c *gin.Context) {
	var sh Shop
	if err := c.ShouldBindJSON(&sh); err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	ctx := c.Request.Context()
	res, err := a.DB.ExecContext(ctx, `INSERT INTO tb_shop (name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		sh.Name, sh.TypeID, sh.Images, sh.Area, sh.Address, sh.X, sh.Y, sh.AvgPrice, sh.Sold, sh.Comments, sh.Score, sh.OpenHours)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("保存失败"))
		return
	}
	id, _ := res.LastInsertId()
	c.JSON(http.StatusOK, dto.OkData(id))
}

func (a *App) putShop(c *gin.Context) {
	var sh Shop
	if err := c.ShouldBindJSON(&sh); err != nil || sh.ID == 0 {
		c.JSON(http.StatusOK, dto.Fail("商铺id不能为空"))
		return
	}
	ctx := c.Request.Context()
	_, err := a.DB.ExecContext(ctx, `UPDATE tb_shop SET name=?,type_id=?,images=?,area=?,address=?,x=?,y=?,avg_price=?,sold=?,comments=?,score=?,open_hours=? WHERE id=?`,
		sh.Name, sh.TypeID, sh.Images, sh.Area, sh.Address, sh.X, sh.Y, sh.AvgPrice, sh.Sold, sh.Comments, sh.Score, sh.OpenHours, sh.ID)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("更新失败"))
		return
	}
	_ = a.RDB.Del(ctx, cacheShopKey+strconv.FormatInt(sh.ID, 10)).Err()
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) getShopOfType(c *gin.Context) {
	typeID, _ := strconv.Atoi(c.Query("typeId"))
	current, _ := strconv.Atoi(c.DefaultQuery("current", "1"))
	if current < 1 {
		current = 1
	}
	xq := c.Query("x")
	yq := c.Query("y")
	ctx := c.Request.Context()

	if xq == "" || yq == "" {
		offset := (current - 1) * 8
		var shops []Shop
		_ = a.DB.SelectContext(ctx, &shops, `SELECT id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time FROM tb_shop WHERE type_id=? ORDER BY id LIMIT 8 OFFSET ?`, typeID, offset)
		var total int64
		_ = a.DB.GetContext(ctx, &total, `SELECT COUNT(*) FROM tb_shop WHERE type_id=?`, typeID)
		pages := (total + 7) / 8
		c.JSON(http.StatusOK, dto.OkData(ShopPage{Records: shops, Total: total, Size: 8, Current: int64(current), Pages: pages}))
		return
	}
	x, _ := strconv.ParseFloat(xq, 64)
	y, _ := strconv.ParseFloat(yq, 64)
	geoKey := shopGeoKeyPref + strconv.Itoa(typeID)
	a.ensureShopGeo(ctx, typeID, geoKey)

	end := current * 8
	from := (current - 1) * 8
	locs, err := a.RDB.GeoRadius(ctx, geoKey, x, y, &redis.GeoRadiusQuery{
		Radius:   500000000,
		Unit:     "m",
		WithDist: true,
		Count:    end,
		Sort:     "ASC",
	}).Result()
	if err != nil || len(locs) <= from {
		c.JSON(http.StatusOK, dto.OkData([]Shop{}))
		return
	}
	locs = locs[from:]
	if len(locs) == 0 {
		c.JSON(http.StatusOK, dto.OkData([]Shop{}))
		return
	}
	ids := make([]int64, 0, len(locs))
	dist := map[int64]float64{}
	for _, loc := range locs {
		sid, _ := strconv.ParseInt(loc.Name, 10, 64)
		ids = append(ids, sid)
		dist[sid] = loc.Dist
	}
	order := make([]string, len(ids))
	for i, id := range ids {
		order[i] = strconv.FormatInt(id, 10)
	}
	inClause := strings.Join(order, ",")
	var shops []Shop
	q := `SELECT id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time FROM tb_shop WHERE id IN (` + inClause + `) ORDER BY FIELD(id,` + inClause + `)`
	_ = a.DB.SelectContext(ctx, &shops, q)
	for i := range shops {
		if d, ok := dist[shops[i].ID]; ok {
			shops[i].Distance = d
		}
	}
	c.JSON(http.StatusOK, dto.OkData(shops))
}

func (a *App) ensureShopGeo(ctx context.Context, typeID int, geoKey string) {
	n, _ := a.RDB.ZCard(ctx, geoKey).Result()
	if n > 0 {
		return
	}
	var shops []Shop
	_ = a.DB.SelectContext(ctx, &shops, `SELECT id,x,y FROM tb_shop WHERE type_id=?`, typeID)
	if len(shops) == 0 {
		return
	}
	for _, sh := range shops {
		_ = a.RDB.GeoAdd(ctx, geoKey, &redis.GeoLocation{
			Longitude: sh.X,
			Latitude:  sh.Y,
			Name:      strconv.FormatInt(sh.ID, 10),
		}).Err()
	}
}

func (a *App) getShopOfName(c *gin.Context) {
	name := c.Query("name")
	current, _ := strconv.Atoi(c.DefaultQuery("current", "1"))
	if current < 1 {
		current = 1
	}
	offset := (current - 1) * 10
	ctx := c.Request.Context()
	var shops []Shop
	if strings.TrimSpace(name) == "" {
		_ = a.DB.SelectContext(ctx, &shops, `SELECT id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time FROM tb_shop ORDER BY id LIMIT 10 OFFSET ?`, offset)
	} else {
		like := "%" + name + "%"
		_ = a.DB.SelectContext(ctx, &shops, `SELECT id,name,type_id,images,area,address,x,y,avg_price,sold,comments,score,open_hours,create_time,update_time FROM tb_shop WHERE name LIKE ? ORDER BY id LIMIT 10 OFFSET ?`, like, offset)
	}
	c.JSON(http.StatusOK, dto.OkData(shops))
}

func (a *App) getShopTypeList(c *gin.Context) {
	ctx := c.Request.Context()
	var types []ShopType
	_ = a.DB.SelectContext(ctx, &types, `SELECT id,name,icon,sort,create_time,update_time FROM tb_shop_type ORDER BY sort`)
	c.JSON(http.StatusOK, dto.OkData(types))
}
