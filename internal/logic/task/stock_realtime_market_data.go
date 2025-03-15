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

// 更新A股实时行情数据-批量
func StockRealTimeMarketDataBatchUpdate(nType int) {

	var (
		bStatus bool
		idx     int
	)
	bStatus = true
	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[实时行情数据-A股] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	// nType: 0-流通市值 1-实时行情数据
	for bStatus == true {
		idx = idx + 1
		bStatus = StockRealTimeMarketDataUpdate(idx, nType, tradeDate.StockDate)
		time.Sleep(time.Second * 2)
	}

	logx.Infof("[实时行情数据-A股] 操作[更新] 状态[完成].")
}

// 更新A股实时行情数据
func StockRealTimeMarketDataUpdate(idx, nType int, tradeDate time.Time) (bStatus bool) {

	strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
	params := url.Values{}
	params.Add("np", "1")
	params.Add("fltt", "1")
	params.Add("invt", "2")
	params.Add("fs", "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048")
	params.Add("fields", "f2,f3,f5,f6,f7,f8,f10,f12,f14,f15,f16,f17,f20,f21")
	params.Add("fid", "f3")                  // f3-涨跌幅 f10-量比
	params.Add("pn", fmt.Sprintf("%v", idx)) // 翻页
	params.Add("pz", "100")                  // 大小
	params.Add("po", "1")
	params.Add("ut", "fa5fd1943c7b386f172d6893dbfba10b")
	params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "push2.eastmoney.com",
		"Referer":         "https://quote.eastmoney.com/center/gridlist.html",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[实时行情数据-A股] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[实时行情数据-A股] 操作[HttpGet] 状态码[%v]error:%s Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[实时行情数据-A股] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[实时行情数据-A股] 操作[data]  error:不存在")
		return
	}
	diff, ok := data["diff"].([]interface{})
	if !ok {
		logx.Errorf("[实时行情数据-A股] 操作[diff] error:不存在")
		return
	}

	// 解析所需字段并更新到数据库
	for _, item := range diff {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["f12"].(string) //股票代码
		stockName, _ := itemMap["f14"].(string) //股票名称

		// 检查 Stock 表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[实时行情数据-A股] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		if nType == 1 {
			currentPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["f2"]))  //最新价
			increaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f3"]))  //涨幅
			volume := decimal.NewFromInt(cast.ToInt64(itemMap["f5"]))            //成交量(万手)
			turnover := decimal.NewFromFloat(cast.ToFloat64(itemMap["f6"]))      //成交额(亿)
			amplitude := decimal.NewFromFloat(cast.ToFloat64(itemMap["f7"]))     //振幅
			turnoverRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f8"]))  //换手
			volumeRatio := decimal.NewFromFloat(cast.ToFloat64(itemMap["f10"]))  //量比
			highestPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["f15"])) //最高价
			lowestPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["f16"]))  //最低价
			openingPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["f17"])) //开盘价

			mData := model.StockDailyMarket{
				StockCode:    stockCode,
				StockName:    stockName,
				Turnover:     turnover.DivRound(decimal.NewFromInt(100000000), 2).InexactFloat64(),
				VolumeRatio:  volumeRatio.Div(decimal.NewFromInt(100)).InexactFloat64(),
				TurnoverRate: turnoverRate.Div(decimal.NewFromInt(100)).InexactFloat64(),
				IncreaseRate: increaseRate.Div(decimal.NewFromInt(100)).InexactFloat64(),
				Amplitude:    amplitude.Div(decimal.NewFromInt(100)).InexactFloat64(),
				CurrentPrice: currentPrice.Div(decimal.NewFromInt(100)).InexactFloat64(),
				OpeningPrice: openingPrice.Div(decimal.NewFromInt(100)).InexactFloat64(),
				HighestPrice: highestPrice.Div(decimal.NewFromInt(100)).InexactFloat64(),
				LowestPrice:  lowestPrice.Div(decimal.NewFromInt(100)).InexactFloat64(),
				Volume:       volume.DivRound(decimal.NewFromInt(10000), 1).InexactFloat64(),
				KlineType:    0, //K线类型(0-日K线,1-周K线,2-月K线)
				TradingDate:  tradeDate,
			}
			err = dao.StockDailyMarket.Save(&mData)
			if err != nil {
				logx.Errorf("[实时行情数据-A股] [数据库]表[StockDailyMarket] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
				return
			}
		} else {
			totalMarketValue := decimal.NewFromInt(cast.ToInt64(itemMap["f20"]))       //总市值
			circulatingMarketValue := decimal.NewFromInt(cast.ToInt64(itemMap["f21"])) //流通市值
			totalMarketValue = totalMarketValue.DivRound(decimal.NewFromInt(100000000), 2)
			circulatingMarketValue = circulatingMarketValue.DivRound(decimal.NewFromInt(100000000), 2)
			nPlateType := int64(0)
			// 盘股类型(0-全部,1-微小盘,2-小盘,3-中盘,4-大盘)
			if circulatingMarketValue.GreaterThanOrEqual(decimal.NewFromInt(20)) && circulatingMarketValue.LessThanOrEqual(decimal.NewFromInt(100)) { // 大于20亿 & 小于100亿  小盘股
				nPlateType = 2
			} else if circulatingMarketValue.GreaterThanOrEqual(decimal.NewFromInt(100)) && circulatingMarketValue.LessThanOrEqual(decimal.NewFromInt(500)) { // 大于100亿 & 小于500亿  中盘股
				nPlateType = 3
			} else if circulatingMarketValue.GreaterThanOrEqual(decimal.NewFromInt(500)) { // 大于500亿 大盘股
				nPlateType = 4
			} else {
				nPlateType = 1
			}

			info, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Updates(model.Stock{
				TotalMarketValue:       totalMarketValue.InexactFloat64(),
				CirculatingMarketValue: circulatingMarketValue.InexactFloat64(),
				PlateType:              nPlateType, //盘股类型(0-全部,1-小盘,2-中盘,3-大盘)
				UpdatedAt:              time.Now(),
			})
			if err != nil {
				logx.Errorf("[实时行情数据-A股] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
				continue
			}

			if info.RowsAffected < 1 {
				logx.Errorf("[实时行情数据-A股] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:更新无效", stockCode)
				continue
			}
		}
	}

	if len(diff) == 100 {
		bStatus = true
	}

	return
}
