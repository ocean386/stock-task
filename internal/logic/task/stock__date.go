package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/zeromicro/go-zero/core/logx"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

// 更新A股交易日期(每月15号更新下个月交易日期)
func StockDateUpdate() {

	// 构建请求参数
	nextMonth := time.Now().AddDate(0, 1, 0).Format("2006-01")
	params := url.Values{}
	params.Add("random", fmt.Sprintf("%.16f", rand.Float64()))
	params.Add("month", nextMonth)

	// 构建完整URL
	strUrl := "https://www.szse.cn/api/report/exchange/onepersistenthour/monthList"
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "application/json, text/javascript, */*; q=0.01",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "www.szse.cn",
		"Referer":         "https://www.szse.cn/aboutus/calendar/",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}

	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[更新A股交易日期] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[更新A股交易日期] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应 JSON
	//var respData []struct {
	//	TradeDate string `json:"trade_date"`
	//}
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[更新A股交易日期] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	dateList, ok := respData["data"].([]interface{})
	if !ok {
		logx.Errorf("[更新A股交易日期] 操作[data] error:不存在")
		return
	}

	for _, item := range dateList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		tradeStatus, _ := itemMap["jybz"].(string)
		if tradeStatus == "0" {
			continue
		}

		tradeDate, _ := itemMap["jyrq"].(string)
		if tradeDate == "" {
			continue
		}

		tData, err := time.Parse("2006-01-02", tradeDate)
		if err != nil {
			logx.Errorf("[更新A股交易日期] 操作[日期解析] 日期字符串[%s] error:%v", tradeDate, err)
			return
		}

		date := &model.StockDate{StockDate: tData}
		err = dao.StockDate.Save(date)
		if err != nil {
			logx.Errorf("[更新A股交易日期] [数据库]表[StockDate] 操作[插入] error:%v", err)
			return
		}
	}
}
