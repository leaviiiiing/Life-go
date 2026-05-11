package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
	"github.com/leaviiiiing/Life-go/server/internal/middleware"
)

const (
	blogLikedKey = "blog:liked:"
	feedKey      = "feed:"
	blogFeedTop  = "blog.feed.topic"
)

func (a *App) postBlog(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	var b Blog
	if err := c.ShouldBindJSON(&b); err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	ctx := c.Request.Context()
	res, err := a.DB.ExecContext(ctx, `INSERT INTO tb_blog (shop_id,user_id,title,images,content,liked,comments) VALUES (?,?,?,?,?,?,?)`,
		b.ShopID, u.ID, b.Title, b.Images, b.Content, 0, 0)
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("新增笔记失败！"))
		return
	}
	id, _ := res.LastInsertId()
	msgID := fmt.Sprintf("BLOG_FEED:%d:%s", id, strings.ReplaceAll(uuid.New().String(), "-", ""))
	payload := BlogFeedMessage{BlogID: id, UserID: u.ID, Timestamp: time.Now().UnixMilli()}
	body, _ := json.Marshal(payload)
	headers := []kafka.Header{
		{Key: "MSG_ID", Value: []byte(msgID)},
		{Key: "IDEMPOTENT_KEY", Value: []byte(msgID)},
	}
	_ = a.KW.WriteMessages(ctx, kafka.Message{
		Topic: blogFeedTop,
		Key:   []byte(fmt.Sprint(id)),
		Value: body,
		Headers: headers,
	})
	c.JSON(http.StatusOK, dto.OkData(id))
}

func (a *App) putBlogLike(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	ctx := c.Request.Context()
	key := blogLikedKey + c.Param("id")
	score, err := a.RDB.ZScore(ctx, key, fmt.Sprint(u.ID)).Result()
	if err == goredis.Nil || score == 0 {
		_, err2 := a.DB.ExecContext(ctx, `UPDATE tb_blog SET liked = liked + 1 WHERE id=?`, id)
		if err2 == nil {
			_ = a.RDB.ZAdd(ctx, key, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: fmt.Sprint(u.ID)}).Err()
		}
	} else {
		_, err3 := a.DB.ExecContext(ctx, `UPDATE tb_blog SET liked = liked - 1 WHERE id=?`, id)
		if err3 == nil {
			_, _ = a.RDB.ZRem(ctx, key, fmt.Sprint(u.ID)).Result()
		}
	}
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) getBlogByID(c *gin.Context) {
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	ctx := c.Request.Context()
	var b Blog
	if err := a.DB.GetContext(ctx, &b, `SELECT id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time FROM tb_blog WHERE id=?`, id); err != nil {
		c.JSON(http.StatusOK, dto.Fail("Blog不存在！"))
		return
	}
	var u User
	_ = a.DB.GetContext(ctx, &u, `SELECT id,nick_name,icon FROM tb_user WHERE id=?`, b.UserID)
	b.Name = u.NickName
	b.Icon = u.Icon
	if uu, ok := middleware.UserFromCtx(c); ok {
		sc, e2 := a.RDB.ZScore(ctx, blogLikedKey+c.Param("id"), fmt.Sprint(uu.ID)).Result()
		b.IsLike = e2 == nil && sc != 0
	}
	c.JSON(http.StatusOK, dto.OkData(b))
}

func (a *App) getBlogHot(c *gin.Context) {
	cur, _ := strconv.Atoi(c.DefaultQuery("current", "1"))
	if cur < 1 {
		cur = 1
	}
	offset := (cur - 1) * 10
	ctx := c.Request.Context()
	var blogs []Blog
	_ = a.DB.SelectContext(ctx, &blogs, `SELECT id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time FROM tb_blog ORDER BY liked DESC LIMIT 10 OFFSET ?`, offset)
	ids := map[int64]struct{}{}
	for i := range blogs {
		ids[blogs[i].UserID] = struct{}{}
	}
	if len(ids) == 0 {
		c.JSON(http.StatusOK, dto.OkData(blogs))
		return
	}
	// load users
	uids := make([]int64, 0, len(ids))
	for id := range ids {
		uids = append(uids, id)
	}
	var users []User
	in := int64Join(uids)
	_ = a.DB.SelectContext(ctx, &users, `SELECT id,nick_name,icon FROM tb_user WHERE id IN (`+in+`)`)
	um := map[int64]User{}
	for _, u := range users {
		um[u.ID] = u
	}
	for i := range blogs {
		if u, ok := um[blogs[i].UserID]; ok {
			blogs[i].Name = u.NickName
			blogs[i].Icon = u.Icon
		}
		if uu, ok := middleware.UserFromCtx(c); ok {
			sc, e2 := a.RDB.ZScore(ctx, blogLikedKey+strconv.FormatInt(blogs[i].ID, 10), fmt.Sprint(uu.ID)).Result()
			blogs[i].IsLike = e2 == nil && sc != 0
		}
	}
	c.JSON(http.StatusOK, dto.OkData(blogs))
}

func int64Join(ids []int64) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(id, 10))
	}
	return b.String()
}

func (a *App) getBlogLikes(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()
	top, err := a.RDB.ZRange(ctx, blogLikedKey+id, 0, 4).Result()
	if err != nil || len(top) == 0 {
		c.JSON(http.StatusOK, dto.OkData([]gin.H{}))
		return
	}
	uids := make([]int64, 0, len(top))
	for _, s := range top {
		v, _ := strconv.ParseInt(s, 10, 64)
		uids = append(uids, v)
	}
	in := int64Join(uids)
	var users []User
	_ = a.DB.SelectContext(ctx, &users, `SELECT id,nick_name,icon FROM tb_user WHERE id IN (`+in+`) ORDER BY FIELD(id,`+in+`)`)
	out := make([]gin.H, 0, len(users))
	for _, u := range users {
		out = append(out, gin.H{"id": u.ID, "nickName": u.NickName, "icon": u.Icon})
	}
	c.JSON(http.StatusOK, dto.OkData(out))
}

func (a *App) getBlogOfMe(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	cur, _ := strconv.Atoi(c.DefaultQuery("current", "1"))
	if cur < 1 {
		cur = 1
	}
	offset := (cur - 1) * 10
	ctx := c.Request.Context()
	var blogs []Blog
	_ = a.DB.SelectContext(ctx, &blogs, `SELECT id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time FROM tb_blog WHERE user_id=? ORDER BY id DESC LIMIT 10 OFFSET ?`, u.ID, offset)
	c.JSON(http.StatusOK, dto.OkData(blogs))
}

func (a *App) getBlogOfUser(c *gin.Context) {
	uid, _ := strconv.ParseInt(c.Query("id"), 10, 64)
	cur, _ := strconv.Atoi(c.DefaultQuery("current", "1"))
	if cur < 1 {
		cur = 1
	}
	offset := (cur - 1) * 10
	ctx := c.Request.Context()
	var blogs []Blog
	_ = a.DB.SelectContext(ctx, &blogs, `SELECT id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time FROM tb_blog WHERE user_id=? ORDER BY id DESC LIMIT 10 OFFSET ?`, uid, offset)
	c.JSON(http.StatusOK, dto.OkData(blogs))
}

func (a *App) getBlogOfFollow(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	maxScore, _ := strconv.ParseFloat(c.Query("lastId"), 64)
	off, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	ctx := c.Request.Context()
	key := feedKey + strconv.FormatInt(u.ID, 10)
	zs, err := a.RDB.ZRevRangeByScoreWithScores(ctx, key, &goredis.ZRangeBy{
		Min:    "0",
		Max:    strconv.FormatFloat(maxScore, 'f', -1, 64),
		Offset: int64(off),
		Count:  4,
	}).Result()
	if err != nil || len(zs) == 0 {
		c.JSON(http.StatusOK, dto.Ok())
		return
	}
	blogIDs := make([]int64, 0, len(zs))
	var minTime int64
	os := 1
	for i, z := range zs {
		bid, _ := strconv.ParseInt(z.Member.(string), 10, 64)
		blogIDs = append(blogIDs, bid)
		t := int64(z.Score)
		if i == 0 {
			minTime = t
			os = 1
			continue
		}
		if t == minTime {
			os++
		} else {
			minTime = t
			os = 1
		}
	}
	in := int64Join(blogIDs)
	var blogs []Blog
	_ = a.DB.SelectContext(ctx, &blogs, `SELECT id,shop_id,user_id,title,images,content,liked,comments,create_time,update_time FROM tb_blog WHERE id IN (`+in+`) ORDER BY FIELD(id,`+in+`)`)
	for i := range blogs {
		var uu User
		_ = a.DB.GetContext(ctx, &uu, `SELECT id,nick_name,icon FROM tb_user WHERE id=?`, blogs[i].UserID)
		blogs[i].Name = uu.NickName
		blogs[i].Icon = uu.Icon
		if cur, ok := middleware.UserFromCtx(c); ok {
			sc, _ := a.RDB.ZScore(ctx, blogLikedKey+strconv.FormatInt(blogs[i].ID, 10), fmt.Sprint(cur.ID)).Result()
			blogs[i].IsLike = sc != 0
		}
	}
	c.JSON(http.StatusOK, dto.OkData(ScrollResult{List: blogs, MinTime: minTime, Offset: os}))
}

func (a *App) putFollow(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	fid, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	isF := c.Param("isFollow") == "true"
	ctx := c.Request.Context()
	key := "follows:" + strconv.FormatInt(u.ID, 10)
	if isF {
		okm, _ := a.RDB.SIsMember(ctx, key, strconv.FormatInt(fid, 10)).Result()
		if okm {
			c.JSON(http.StatusOK, dto.Fail("已关注！"))
			return
		}
		_, err := a.DB.ExecContext(ctx, `INSERT INTO tb_follow (user_id,follow_user_id) VALUES (?,?)`, u.ID, fid)
		if err != nil {
			c.JSON(http.StatusOK, dto.Fail("您已经关注过该用户了"))
			return
		}
		_ = a.RDB.SAdd(ctx, key, strconv.FormatInt(fid, 10)).Err()
	} else {
		okm, _ := a.RDB.SIsMember(ctx, key, strconv.FormatInt(fid, 10)).Result()
		if !okm {
			c.JSON(http.StatusOK, dto.Fail("未关注！"))
			return
		}
		_, err := a.DB.ExecContext(ctx, `DELETE FROM tb_follow WHERE user_id=? AND follow_user_id=?`, u.ID, fid)
		if err != nil {
			c.JSON(http.StatusOK, dto.Fail("取关失败！"))
			return
		}
		_, _ = a.RDB.SRem(ctx, key, strconv.FormatInt(fid, 10)).Result()
	}
	c.JSON(http.StatusOK, dto.Ok())
}

func (a *App) getFollowOrNot(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	fid, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	okm, _ := a.RDB.SIsMember(c.Request.Context(), "follows:"+strconv.FormatInt(u.ID, 10), strconv.FormatInt(fid, 10)).Result()
	c.JSON(http.StatusOK, dto.OkData(okm))
}

func (a *App) getFollowCommon(c *gin.Context) {
	u, ok := middleware.UserFromCtx(c)
	if !ok {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	oid, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	ctx := c.Request.Context()
	ids, err := a.RDB.SInter(ctx, "follows:"+strconv.FormatInt(u.ID, 10), "follows:"+strconv.FormatInt(oid, 10)).Result()
	if err != nil || len(ids) == 0 {
		c.JSON(http.StatusOK, dto.OkData([]gin.H{}))
		return
	}
	uids := make([]int64, 0, len(ids))
	for _, s := range ids {
		v, _ := strconv.ParseInt(s, 10, 64)
		uids = append(uids, v)
	}
	in := int64Join(uids)
	var users []User
	_ = a.DB.SelectContext(ctx, &users, `SELECT id,nick_name,icon FROM tb_user WHERE id IN (`+in+`)`)
	out := make([]gin.H, 0, len(users))
	for _, u := range users {
		out = append(out, gin.H{"id": u.ID, "nickName": u.NickName, "icon": u.Icon})
	}
	c.JSON(http.StatusOK, dto.OkData(out))
}
