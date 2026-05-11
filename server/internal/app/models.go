package app

import "time"

type User struct {
	ID        int64     `json:"id" db:"id"`
	Phone     string    `json:"phone,omitempty" db:"phone"`
	Password  string    `json:"-" db:"password"`
	NickName  string    `json:"nickName" db:"nick_name"`
	Icon      string    `json:"icon" db:"icon"`
	CreateTime time.Time `json:"createTime,omitempty" db:"create_time"`
	UpdateTime time.Time `json:"updateTime,omitempty" db:"update_time"`
}

type UserInfo struct {
	ID         int64      `json:"id,omitempty" db:"id"`
	UserID     int64      `json:"userId,omitempty" db:"user_id"`
	City       string     `json:"city,omitempty" db:"city"`
	Introduce  string     `json:"introduce,omitempty" db:"introduce"`
	Gender     int        `json:"gender,omitempty" db:"gender"`
	Birthday   *time.Time `json:"birthday,omitempty" db:"birthday"`
	CreateTime *time.Time `json:"createTime,omitempty" db:"create_time"`
	UpdateTime *time.Time `json:"updateTime,omitempty" db:"update_time"`
}

type Shop struct {
	ID         int64     `json:"id" db:"id"`
	Name       string    `json:"name" db:"name"`
	TypeID     int64     `json:"typeId" db:"type_id"`
	Images     string    `json:"images" db:"images"`
	Area       string    `json:"area,omitempty" db:"area"`
	Address    string    `json:"address" db:"address"`
	X          float64   `json:"x" db:"x"`
	Y          float64   `json:"y" db:"y"`
	AvgPrice   int64     `json:"avgPrice,omitempty" db:"avg_price"`
	Sold       int       `json:"sold" db:"sold"`
	Comments   int       `json:"comments" db:"comments"`
	Score      int       `json:"score" db:"score"`
	OpenHours  string    `json:"openHours,omitempty" db:"open_hours"`
	CreateTime time.Time `json:"createTime,omitempty" db:"create_time"`
	UpdateTime time.Time `json:"updateTime,omitempty" db:"update_time"`
	Distance   float64   `json:"distance,omitempty" db:"-"`
}

type ShopType struct {
	ID         int64     `json:"id" db:"id"`
	Name       string    `json:"name" db:"name"`
	Icon       string    `json:"icon" db:"icon"`
	Sort       int       `json:"sort" db:"sort"`
	CreateTime time.Time `json:"createTime,omitempty" db:"create_time"`
	UpdateTime time.Time `json:"updateTime,omitempty" db:"update_time"`
}

type Blog struct {
	ID         int64     `json:"id" db:"id"`
	ShopID     int64     `json:"shopId" db:"shop_id"`
	UserID     int64     `json:"userId" db:"user_id"`
	Icon       string    `json:"icon,omitempty" db:"-"`
	Name       string    `json:"name,omitempty" db:"-"`
	IsLike     bool      `json:"isLike,omitempty" db:"-"`
	Title      string    `json:"title" db:"title"`
	Images     string    `json:"images" db:"images"`
	Content    string    `json:"content" db:"content"`
	Liked      int       `json:"liked" db:"liked"`
	Comments   *int      `json:"comments,omitempty" db:"comments"`
	CreateTime time.Time `json:"createTime" db:"create_time"`
	UpdateTime time.Time `json:"updateTime" db:"update_time"`
}

type Voucher struct {
	ID          int64      `json:"id,omitempty" db:"id"`
	ShopID      int64      `json:"shopId" db:"shop_id"`
	Title       string     `json:"title" db:"title"`
	SubTitle    string     `json:"subTitle,omitempty" db:"sub_title"`
	Rules       string     `json:"rules,omitempty" db:"rules"`
	PayValue    int64      `json:"payValue" db:"pay_value"`
	ActualValue int64      `json:"actualValue" db:"actual_value"`
	Type        int        `json:"type" db:"type"`
	Status      int        `json:"status,omitempty" db:"status"`
	Stock       *int       `json:"stock,omitempty" db:"stock"`
	BeginTime   *time.Time `json:"beginTime,omitempty" db:"begin_time"`
	EndTime     *time.Time `json:"endTime,omitempty" db:"end_time"`
	CreateTime  time.Time  `json:"createTime,omitempty" db:"create_time"`
	UpdateTime  time.Time  `json:"updateTime,omitempty" db:"update_time"`
}

type VoucherOrder struct {
	ID         int64      `json:"id" db:"id"`
	UserID     int64      `json:"userId" db:"user_id"`
	VoucherID  int64      `json:"voucherId" db:"voucher_id"`
	PayType    int        `json:"payType,omitempty" db:"pay_type"`
	Status     int        `json:"status,omitempty" db:"status"`
	CreateTime time.Time  `json:"createTime,omitempty" db:"create_time"`
	PayTime    *time.Time `json:"payTime,omitempty" db:"pay_time"`
	UseTime    *time.Time `json:"useTime,omitempty" db:"use_time"`
	RefundTime *time.Time `json:"refundTime,omitempty" db:"refund_time"`
	UpdateTime time.Time  `json:"updateTime,omitempty" db:"update_time"`
}

type MqKafkaLog struct {
	ID           int64     `json:"id" db:"id"`
	MsgID        string    `json:"msgId" db:"msg_id"`
	BizType      string    `json:"bizType,omitempty" db:"biz_type"`
	BizKey       string    `json:"bizKey,omitempty" db:"biz_key"`
	Topic        string    `json:"topic,omitempty" db:"topic"`
	PartitionID  *int      `json:"partitionId,omitempty" db:"partition_id"`
	OffsetVal    *int64    `json:"offsetVal,omitempty" db:"offset_val"`
	Direction    string    `json:"direction" db:"direction"`
	Status       string    `json:"status" db:"status"`
	ErrorMsg     string    `json:"errorMsg,omitempty" db:"error_msg"`
	CreateTime   time.Time `json:"createTime" db:"create_time"`
}

type VoucherOrderRepublishRequest struct {
	OrderID   int64 `json:"orderId"`
	UserID    int64 `json:"userId"`
	VoucherID int64 `json:"voucherId"`
}

type ScrollResult struct {
	List    interface{} `json:"list"`
	MinTime int64       `json:"minTime"`
	Offset  int         `json:"offset"`
}

type BlogFeedMessage struct {
	BlogID    int64 `json:"blogId"`
	UserID    int64 `json:"userId"`
	Timestamp int64 `json:"timestamp"`
}

type ShopPage struct {
	Records interface{} `json:"records"`
	Total   int64       `json:"total"`
	Size    int64       `json:"size"`
	Current int64       `json:"current"`
	Pages   int64       `json:"pages"`
}
