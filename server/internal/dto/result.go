package dto

// Result matches Java com.hmdp.dto.Result JSON for the main API.
type Result struct {
	Success  bool        `json:"success"`
	ErrorMsg *string     `json:"errorMsg,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	Total    *int64      `json:"total,omitempty"`
}

func Ok() Result {
	return Result{Success: true}
}

func OkData(data interface{}) Result {
	return Result{Success: true, Data: data}
}

func OkList(data interface{}, total int64) Result {
	return Result{Success: true, Data: data, Total: &total}
}

func Fail(msg string) Result {
	return Result{Success: false, ErrorMsg: &msg}
}

func StrPtr(s string) *string { return &s }
