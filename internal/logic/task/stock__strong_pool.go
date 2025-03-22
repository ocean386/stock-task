package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"net/url"
	"time"
)

// 更新每日强势榜-批量 强势榜(涨停股,跌停股,炸板股,强势股)
func StockStrongPoolBatchUpdate() {
	var (
		bStatus bool
		strType string
		strSort string
		idx     int64
	)
	bStatus = true
	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[更新每日强势榜] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	mapType := map[int64]string{
		0: "getTopicZTPool", //涨停股 fbt:asc
		1: "getTopicDTPool", //跌停股 fund:asc
		2: "getTopicZBPool", //炸板股 fbt:asc
		3: "getTopicQSPool", //强势股 zdp:desc
	}

	mapSort := map[int64]string{
		0: "fbt:asc",  //涨停股 fbt:asc
		1: "fund:asc", //跌停股 fund:asc
		2: "fbt:asc",  //炸板股 fbt:asc
		3: "zdp:desc", //强势股 zdp:desc
	}

	for bStatus == true {
		strType = mapType[idx]
		strSort = mapSort[idx]
		idx = idx + 1
		bStatus = StockStrongPoolUpdate(strType, strSort, idx, tradeDate.StockDate)
		time.Sleep(time.Millisecond * 100)
	}

	logx.Infof("[更新每日强势榜] 操作[更新] 状态[完成].")
}

// 强势榜(涨停股,跌停股,炸板股,强势股)
func StockStrongPoolUpdate(strType, strSort string, nType int64, tradeDate time.Time) (bStatus bool) {

	strUrl := fmt.Sprintf("https://push2ex.eastmoney.com/%s", strType)
	params := url.Values{}
	params.Add("ut", "7eea3edcaed734bea9cbfc24409ed989")
	params.Add("dpt", "wz.ztzt")
	params.Add("Pageindex", "0")
	params.Add("pagesize", "500")
	params.Add("sort", strSort)
	params.Add("date", tradeDate.Format("20060102"))
	params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())
	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "push2ex.eastmoney.com",
		"Referer":         "https://quote.eastmoney.com/ztb/detail",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
	}

	// 发送HTTP请求
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[更新每日强势榜] 操作[HttpGet] error:%s  Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[更新每日强势榜] 操作[HttpGet] 状态码[%v]", statusCode)
		return
	}

	// 解析响应数据
	var respData map[string]interface{}
	if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
		logx.Errorf("[更新每日强势榜] 操作[JsonUnmarshal] error:%s", err.Error())
		return
	}

	// 处理数据
	poolData, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[更新每日强势榜] 操作[data] error:数据格式错误")
		return
	}

	poolList, ok := poolData["pool"].([]interface{})
	if !ok {
		logx.Errorf("[更新每日强势榜] 操作[pool] error:数据格式错误")
		return
	}

	for _, item := range poolList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["c"].(string) //股票代码
		//检查Stock表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[更新每日强势榜] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		var (
			strFirstTime                  string
			strLastTime                   string
			fund                          decimal.Decimal
			circulatingMarketValue        decimal.Decimal
			turnover                      decimal.Decimal
			dSealingOrderTradingRatio     decimal.Decimal
			dSealingOrderCirculatingRatio decimal.Decimal
			nBreakingBoardTimes           int64
			nNewHigh                      int64
			nConsecutiveLimitUpDays       int64
			nUpdownDays                   int64
			nUpdownTimes                  int64
		)

		turnoverRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["hs"]))                                     //换手
		increaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["zdp"]))                                    //涨幅
		currentPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["p"])).DivRound(decimal.NewFromInt(100), 2) //最新价

		circulatingMarketValue = decimal.NewFromFloat(cast.ToFloat64(itemMap["ltsz"])) //流通市值
		turnover = decimal.NewFromFloat(cast.ToFloat64(itemMap["amount"]))             //成交金额

		if nType == 1 { //涨停
			fund = decimal.NewFromFloat(cast.ToFloat64(itemMap["fund"])) //封板资金
			strFirstTime = cast.ToString(itemMap["fbt"])                 //首次封板时间
			strLastTime = cast.ToString(itemMap["lbt"])                  //最后封板时间

			if len(strFirstTime) != 6 {
				strFirstTime = "0" + strFirstTime
			}
			strFirstTime = strFirstTime[0:2] + ":" + strFirstTime[2:4] + ":" + strFirstTime[4:6]

			if len(strLastTime) != 6 {
				strLastTime = "0" + strLastTime
			}
			strLastTime = strLastTime[0:2] + ":" + strLastTime[2:4] + ":" + strLastTime[4:6]

			nBreakingBoardTimes = cast.ToInt64(itemMap["zbc"])     //炸板次数
			nConsecutiveLimitUpDays = cast.ToInt64(itemMap["lbc"]) //连板次数
			zttj, ok := itemMap["zttj"].(map[string]interface{})
			if !ok {
				continue
			}

			//涨停统计-天数
			nUpdownDays = cast.ToInt64(zttj["days"])
			//涨停统计-次数
			nUpdownTimes = cast.ToInt64(zttj["ct"])
			//封单成交比
			dSealingOrderTradingRatio = fund.Div(turnover).Round(2)
			//封单流通比
			dSealingOrderCirculatingRatio = fund.Div(circulatingMarketValue).Round(2)
		} else if nType == 2 { //跌停
			fund = decimal.NewFromFloat(cast.ToFloat64(itemMap["fund"])) //封板资金
			strLastTime = cast.ToString(itemMap["lbt"])                  //最后封板时间
			if len(strLastTime) != 6 {
				strLastTime = "0" + strLastTime
			}
			strLastTime = strLastTime[0:2] + ":" + strLastTime[2:4] + ":" + strLastTime[4:6]
			//跌停统计-天数
			nUpdownDays = cast.ToInt64(itemMap["days"])
			//跌停统计-开板次数
			nUpdownTimes = cast.ToInt64(itemMap["oc"])
		} else if nType == 3 { //炸板
			strFirstTime = cast.ToString(itemMap["fbt"]) //首次封板时间

			if len(strFirstTime) != 6 {
				strFirstTime = "0" + strFirstTime
			}
			strFirstTime = strFirstTime[0:2] + ":" + strFirstTime[2:4] + ":" + strFirstTime[4:6]

			nBreakingBoardTimes = cast.ToInt64(itemMap["zbc"]) //炸板次数
			zttj, ok := itemMap["zttj"].(map[string]interface{})
			if !ok {
				continue
			}

			//涨停统计-天数
			nUpdownDays = cast.ToInt64(zttj["days"])
			//涨停统计-次数
			nUpdownTimes = cast.ToInt64(zttj["ct"])
		} else { //强势股
			nNewHigh = cast.ToInt64(itemMap["nh"]) //是否新高(0-否 1-是)
			zttj, ok := itemMap["zttj"].(map[string]interface{})
			if !ok {
				continue
			}

			//涨停统计-天数
			nUpdownDays = cast.ToInt64(zttj["days"])
			//涨停统计-次数
			nUpdownTimes = cast.ToInt64(zttj["ct"])
		}

		stock := model.StockStrong{
			StockCode:                    stockCode,
			StockName:                    rData.StockName,
			CirculatingMarketValue:       circulatingMarketValue.DivRound(decimal.NewFromInt(100000000), 2).InexactFloat64(), //流通市值
			PlateType:                    rData.PlateType,
			TurnoverRate:                 turnoverRate.Round(2).InexactFloat64(),                           //换手率
			IncreaseRate:                 increaseRate.Round(2).InexactFloat64(),                           //涨幅
			CurrentPrice:                 currentPrice.InexactFloat64(),                                    //最新价格
			NewHigh:                      nNewHigh,                                                         //是否新高(0-否 1-是)
			UpdownType:                   nType,                                                            //涨跌类型(0-全部,1-涨停股,2-跌停股,3-炸板股,4-强势股)
			SealingFund:                  fund.DivRound(decimal.NewFromInt(100000000), 2).InexactFloat64(), //封板资金
			FirstSealingTime:             strFirstTime,                                                     //首次封板时间
			LastSealingTime:              strLastTime,                                                      //最后封板时间
			BreakingBoardTimes:           nBreakingBoardTimes,                                              //炸板次数
			UpdownDays:                   nUpdownDays,                                                      //涨停统计-天数
			UpdownTimes:                  nUpdownTimes,                                                     //涨停统计-次数
			ConsecutiveLimitUpDays:       nConsecutiveLimitUpDays,                                          //连板次数
			SealingOrderTradingRatio:     dSealingOrderTradingRatio.InexactFloat64(),                       //封单成交比
			SealingOrderCirculatingRatio: dSealingOrderCirculatingRatio.InexactFloat64(),                   //封单流通比
			TradingDate:                  tradeDate,
			UpdatedAt:                    time.Now(),
		}

		if err := dao.StockStrong.Save(&stock); err != nil {
			logx.Errorf("[更新每日强势榜] [数据库]表[StockStrong] 操作[保存] 股票代码[%v]-error:%v", stock.StockCode, err)
			return
		}

	}

	if nType == 4 {
		return false
	}

	return true
}
