package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func IsToday(t time.Time) bool {
	now := time.Now()
	return t.Year() == now.Year() && t.YearDay() == now.YearDay()
}

func IsThisMonth(t time.Time) bool {
	now := time.Now()
	return t.Year() == now.Year() && t.Month() == now.Month()
}

// 更新A股日K线行情数据-批量(每个交易日执行一次,每周六执行周期级别,每月1号执行月期级别)
func StockDailyMarketBatchUpdate() {

	stockList, err := dao.Stock.Where(dao.Stock.PlateType.In(1, 2, 3)).Find() //微盘,小盘,中盘
	if err != nil {
		logx.Errorf("[历史行情数据-A股] [数据库]表[Stock] 操作[查询]-error:%s", err.Error())
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
				logx.Errorf("[历史行情数据-A股] [数据库]表[StockDailyMarket] 操作[查询]-error:%s", err.Error())
				return
			}

			var (
				strBeginDate string
				strSecID     string
				strCode      string
			)
			if err == nil {
				if IsToday(marketData.TradingDate) {
					continue
				}
				if klineType == 0 { //日
					marketData.TradingDate = marketData.TradingDate.AddDate(0, 0, 1)
				} else if klineType == 1 { //周
					marketData.TradingDate = marketData.TradingDate.AddDate(0, 0, 7)
				} else { //月
					marketData.TradingDate = marketData.TradingDate.AddDate(0, 1, 0)
				}

				strBeginDate = marketData.TradingDate.Format(time.DateOnly)
				strBeginDate = strings.Replace(strBeginDate, "-", "", 2)

				if IsToday(marketData.TradingDate) || IsThisMonth(marketData.TradingDate) {
					continue
				}
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

			StockDailyMarketUpdate(strBeginDate, strSecID, strCode, klineType)
			time.Sleep(time.Millisecond * 100)
		}

		klineType = klineType + 1
	}
}

// 更新A股日K线 行情数据
func StockDailyMarketUpdate(strBeginDate, strSecID, strCode string, klineType int64) {

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
		params.Add("beg", "20150101")
	}
	params.Add("end", "20250325")
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
		logx.Errorf("[历史行情数据-A股] 操作[HttpGet] K线级别[%v] error:%s Url地址[%s]", klineType, err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[历史行情数据-A股] 操作[HttpGet] K线级别[%v] 状态码[%v] error:%s Url地址[%s]", klineType, statusCode, fullUrl)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[历史行情数据-A股] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[历史行情数据-A股] 操作[data] error:不存在")
		return
	}

	strStockCode, ok := data["code"].(string)
	if !ok {
		logx.Errorf("[历史行情数据-A股] 操作[code] error:不存在")
		return
	}

	strName, ok := data["name"].(string)
	if !ok {
		logx.Errorf("[历史行情数据-A股] 操作[name] error:不存在")
		return
	}

	kData, ok := data["klines"].([]interface{})
	if !ok {
		logx.Errorf("[历史行情数据-A股] 操作[klines] error:不存在")
		return
	}

	var (
		marketSlice []*model.StockDailyMarket
	)
	// 定义 2015 年 01 月 01 日的时间
	targetDate, _ := time.Parse("2006-01-02", "2015-01-01") //过滤2015-01-01 行情数据
	// 解析每行数据
	for _, kline := range kData {
		klineStr, ok := kline.(string)
		if !ok {
			logx.Errorf("[历史行情数据-A股] 操作[klines] error:Invalid kline data")
			continue
		}
		fields := strings.Split(klineStr, ",")
		if len(fields) != 10 {
			logx.Errorf("[历史行情数据-A股] 操作[分隔符] error:数据不够")
			continue
		}
		date := fields[0] // 交易日期
		tDate, _ := time.Parse(time.DateOnly, date)
		openPrice := cast.ToFloat64(fields[1])                      // 开盘
		closePrice := cast.ToFloat64(fields[2])                     // 现价-收盘价
		highPrice := cast.ToFloat64(fields[3])                      // 最高价
		lowPrice := cast.ToFloat64(fields[4])                       // 最低价
		volume := decimal.NewFromInt(cast.ToInt64(fields[5]))       //成交量(万手)
		turnover := decimal.NewFromFloat(cast.ToFloat64(fields[6])) //成交额(亿)
		amplitude := cast.ToFloat64(fields[7])                      //振幅
		increaseRate := cast.ToFloat64(fields[8])                   //涨幅
		turnoverRate := cast.ToFloat64(fields[9])                   //换手

		if openPrice < 0 || closePrice < 0 || volume.LessThan(decimal.NewFromInt(1)) || tDate.Before(targetDate) {
			continue
		}

		turnover = turnover.DivRound(decimal.NewFromInt(100000000), 2)
		volume = volume.DivRound(decimal.NewFromInt(10000), 1)

		marketData := &model.StockDailyMarket{
			StockCode:    strStockCode,
			StockName:    strName,
			Turnover:     turnover.InexactFloat64(),
			TurnoverRate: turnoverRate,
			IncreaseRate: increaseRate,
			Amplitude:    amplitude,
			CurrentPrice: closePrice,
			OpeningPrice: openPrice,
			HighestPrice: highPrice,
			LowestPrice:  lowPrice,
			Volume:       volume.InexactFloat64(),
			KlineType:    klineType,
			TradingDate:  tDate,
		}

		marketSlice = append(marketSlice, marketData)
	}

	if len(marketSlice) == 0 {
		logx.Infof("[历史行情数据-A股] 股票代码[%v] K线级别[%v] Info:历史行情数据为空", strStockCode, klineType)
		return
	}

	end := 0
	for i := 0; i < len(marketSlice); i = end {
		if len(marketSlice)/1000 == 0 {
			err = dao.StockDailyMarket.Save(marketSlice...)
			if err != nil {
				logx.Errorf("[历史行情数据-A股] [数据库]表[StockDailyMarket] 操作[插入] 股票代码[%v]-error:%v", strStockCode, err)
				return
			}
			break
		} else {
			end = i + 1000
			if end > len(marketSlice) {
				end = len(marketSlice)
			}
			dataSlice := marketSlice[i:end]
			err = dao.StockDailyMarket.Save(dataSlice...) //批量插入数据
			if err != nil {
				logx.Errorf("[历史行情数据-A股][数据库]表[StockDailyMarket] 操作[插入] 股票代码[%v]-error:%v", strStockCode, err)
				return
			}
		}
	}

	logx.Infof("[历史行情数据-A股][数据库]表[StockDailyMarket] 操作[插入] 股票代码[%v] 股票简称[%v] K线级别[%v]", strStockCode, strName, klineType)
}
