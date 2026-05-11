package app

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/leaviiiiing/Life-go/server/internal/dto"
)

func (a *App) postUploadBlog(c *gin.Context) {
	fh, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusOK, dto.Fail("参数错误"))
		return
	}
	base := a.Cfg.UploadDir
	_ = os.MkdirAll(base, 0755)
	name := strings.ReplaceAll(uuid.New().String(), "-", "")
	h := int(hashString(name) & 0xF)
	h2 := int((hashString(name) >> 4) & 0xF)
	suffix := ""
	if i := strings.LastIndex(fh.Filename, "."); i >= 0 {
		suffix = strings.ToLower(fh.Filename[i+1:])
	}
	rel := fmt.Sprintf("/blogs/%d/%d/%s.%s", h, h2, name, suffix)
	dir := filepath.Join(base, "blogs", fmt.Sprint(h), fmt.Sprint(h2))
	_ = os.MkdirAll(dir, 0755)
	dst := filepath.Join(dir, name+"."+suffix)
	if err := c.SaveUploadedFile(fh, dst); err != nil {
		c.JSON(http.StatusOK, dto.Fail("文件上传失败"))
		return
	}
	c.JSON(http.StatusOK, dto.OkData(rel))
}

func hashString(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

func (a *App) getUploadBlogDelete(c *gin.Context) {
	name := c.Query("name")
	if strings.Contains(name, "..") || strings.Contains(name, string(filepath.Separator)) {
		c.JSON(http.StatusOK, dto.Fail("错误的文件名称"))
		return
	}
	p := filepath.Join(a.Cfg.UploadDir, strings.TrimPrefix(name, "/"))
	st, err := os.Stat(p)
	if err != nil || st.IsDir() {
		c.JSON(http.StatusOK, dto.Fail("错误的文件名称"))
		return
	}
	_ = os.Remove(p)
	c.JSON(http.StatusOK, dto.Ok())
}
