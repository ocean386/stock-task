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

// 更新A股交易日期
func StockDateUpdate() {

	logx.Infof("执行 StockHTTP 任务:%v", time.Now().Format("15:04:05"))

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
		logx.Errorf("IsStockNew-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("IsStockNew-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析响应 JSON
	//var respData []struct {
	//	TradeDate string `json:"trade_date"`
	//}
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Infof("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-StatusCode:%v", fullUrl, statusCode)
		return
	}

	dateList, ok := respData["data"].([]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:data", fullUrl)
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
			logx.Errorf("日期解析出错: %v，日期字符串: %s", err, tradeDate)
			return
		}

		date := &model.StockDate{StockDate: tData}
		err = dao.StockDate.Save(date)
		if err != nil {
			logx.Errorf("dao StockDate save db [%v] error:%v", tradeDate, err.Error())
			return
		}
	}
}
