package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 更新A股日K线行情数据-批量
func StockMarketDataBatchUpdate() {

	stockList, err := dao.Stock.Find()
	if err != nil {
		logx.Errorf("dao stock find db error:%s", err.Error())
		return
	}

	var (
		marketData *model.StockDailyMarket
		klineType  int64
	)
	for klineType < 3 {
		for _, s := range stockList {
			marketData, err = dao.StockDailyMarket.Where(dao.StockDailyMarket.StockCode.Eq(s.StockCode), dao.StockDailyMarket.KlineType.Eq(klineType)).Order(dao.StockDailyMarket.TradingDate.Desc()).First()
			if err != nil && err != gorm.ErrRecordNotFound {
				logx.Errorf("MySql StockDailyMarket table find error:%s", err.Error())
				return
			}

			var (
				strBeginDate string
				strSecID     string
				strCode      string
			)
			if err == nil {
				strBeginDate = time.Unix(marketData.TradingDate, 0).Format(time.DateOnly)
				strBeginDate = strings.Replace(strBeginDate, "-", "", 2)
			}

			switch s.Exchange {
			case 1:
				strSecID = fmt.Sprintf("0.%v", s.StockCode)
				strCode = "sz" + s.StockCode
			case 2:
				strSecID = fmt.Sprintf("1.%v", s.StockCode)
				strCode = "sh" + s.StockCode
			case 3:
				strSecID = fmt.Sprintf("0.%v", s.StockCode)
				strCode = "bj" + s.StockCode
			}

			if len(strSecID) == 0 {
				continue
			}
			logx.Infof("Code:%v, Date:%v, strCode:%v, strKlineType:%v ", s.StockCode, strBeginDate, strCode, klineType)
			StockMarketDataUpdate(strBeginDate, strSecID, strCode, klineType)
			time.Sleep(time.Millisecond * 200)
		}

		klineType = klineType + 1
	}
}

// 更新A股日K线 行情数据
func StockMarketDataUpdate(strBeginDate, strSecID, strCode string, klineType int64) {

	var strKlineType string
	switch klineType { //{"日K线": "101", "周K线": "102", "月K线": "103"}
	case 0:
		strKlineType = "101"
	case 1:
		strKlineType = "102"
	case 2:
		strKlineType = "103"
	}

	strUrl := "https://push2his.eastmoney.com/api/qt/stock/kline/get"
	params := url.Values{}
	params.Add("secid", strSecID)
	params.Add("ut", "fa5fd1943c7b386f172d6893dbfba10b")
	params.Add("fields1", "f1,f3")
	params.Add("fields2", "f51,f52,f53,f54,f55,f56,f57,f58,f59,f61")
	params.Add("klt", strKlineType)
	params.Add("fqt", "1")
	if len(strBeginDate) > 0 {
		params.Add("beg", strBeginDate)
	} else {
		params.Add("beg", "20050101")
	}
	params.Add("end", "20300101")
	params.Add("lmt", "10000")
	params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "push2his.eastmoney.com",
		"Referer":         fmt.Sprintf("https://quote.eastmoney.com/%v.html", strCode),
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("RequestHttp-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-StatusCode:%v", fullUrl, statusCode)
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:pageHelp", fullUrl)
		return
	}

	strStockCode, ok := data["code"].(string)
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:code", fullUrl)
		return
	}

	strName, ok := data["name"].(string)
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:name", fullUrl)
		return
	}

	kData, ok := data["klines"].([]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:klines", fullUrl)
		return
	}

	var (
		marketSlice []*model.StockDailyMarket
	)
	// 定义 2005 年 01 月 01 日的时间
	targetDate, _ := time.Parse("2006-01-02", "2005-01-01") //过滤2005-01-01的数据
	// 解析每行数据
	for _, kline := range kData {
		klineStr, ok := kline.(string)
		if !ok {
			logx.Errorf("RequestHttp-http.StatusOK,[%s]-Invalid kline data", fullUrl)
			continue
		}
		fields := strings.Split(klineStr, ",")
		if len(fields) != 10 {
			logx.Errorf("RequestHttp-http.StatusOK,[%s]-Invalid kline fields", fullUrl)
			continue
		}
		date := fields[0] // 交易日期
		tDate, _ := time.Parse(time.DateOnly, date)
		openPrice := cast.ToFloat64(fields[1])    // 开盘
		closePrice := cast.ToFloat64(fields[2])   // 现价-收盘价
		highPrice := cast.ToFloat64(fields[3])    // 最高价
		lowPrice := cast.ToFloat64(fields[4])     // 最低价
		volume := cast.ToInt64(fields[5])         //成交量
		amount := cast.ToFloat64(fields[6])       //成交额
		amplitude := cast.ToFloat64(fields[7])    //振幅
		increaseRate := cast.ToFloat64(fields[8]) //涨幅
		turnoverRate := cast.ToFloat64(fields[9]) //换手

		if openPrice < 0 || closePrice < 0 || volume < 1000 || tDate.Before(targetDate) {
			continue
		}

		marketData := &model.StockDailyMarket{
			StockCode:    strStockCode,
			StockName:    strName,
			Turnover:     amount,
			TurnoverRate: turnoverRate,
			IncreaseRate: increaseRate,
			Amplitude:    amplitude,
			CurrentPrice: closePrice,
			OpeningPrice: openPrice,
			HighestPrice: highPrice,
			LowestPrice:  lowPrice,
			Volume:       volume,
			KlineType:    klineType,
			TradingDate:  tDate.Unix(),
		}

		marketSlice = append(marketSlice, marketData)
	}

	if len(marketSlice) == 0 {
		logx.Infof("股票代码[%v] 股票简称[%v] 历史行情数据为空.", strStockCode, strName)
		return
	}

	size := len(marketSlice) / 1000
	for i := 0; i < size || i == 0; i++ {
		if size == 0 {
			err = dao.StockDailyMarket.Save(marketSlice...)
			if err != nil {
				logx.Errorf("dao StockDate save db [%v] error:%v", strName, err.Error())
				return
			}
		} else {
			end := i + 1000
			if end > len(marketSlice) {
				end = len(marketSlice)
			}
			dataSlice := marketSlice[i:end]
			err = dao.StockDailyMarket.Save(dataSlice...) //批量插入数据
			if err != nil {
				logx.Errorf("dao StockDate save db [%v] error:%v", strName, err.Error())
				return
			}
		}
	}

	logx.Infof("[%v]-[%v] K线级别[%v]", strStockCode, strName, klineType)
}
