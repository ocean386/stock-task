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

// 更新个股资金流排名-批量
func StockFundRankBatchUpdate() {

	var (
		bStatus bool
		pageIdx int64
	)
	bStatus = true
	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[更新个股资金流排名] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	for bStatus == true {
		bStatus = StockFundRankUpdate(pageIdx, tradeDate.StockDate) //净额
		pageIdx = pageIdx + 1
		time.Sleep(time.Millisecond * 200)
	}

	StockMainPercentSort(tradeDate.StockDate) //净占比

	logx.Infof("[更新个股资金流排名] 操作[更新] 状态[完成].")
}

// 主力净流入占比排序
func StockMainPercentSort(tradeDate time.Time) {

	stockFunds, err := dao.StockFundRank.Select(dao.StockFundRank.StockCode).Where(dao.StockFundRank.TradingDate.Eq(tradeDate)).Order(dao.StockFundRank.MainPercent.Desc()).Find()
	if err != nil {
		logx.Errorf("[更新个股资金流排名] [数据库]表[StockFundRank] 操作[查询]-error:%s", err.Error())
		return
	}

	for idx, s := range stockFunds {
		info, err := dao.StockFundRank.Where(dao.StockFundRank.StockCode.Eq(s.StockCode)).Updates(model.StockFundRank{
			FundPercentSortID: int64(idx + 1),
			UpdatedAt:         time.Now(),
		},
		)
		if err != nil {
			logx.Errorf("[更新个股资金流排名] [数据库]表[StockFundRank] 操作[更新] 股票代码[%v]-error:%v", s.StockCode, err)
			return
		}

		if info.RowsAffected < 1 {
			logx.Errorf("[更新个股资金流排名] [数据库]表[StockFundRank] 操作[更新] 股票代码[%v]-error:更新无效", s.StockCode)
			return
		}
	}
}

// 更新个股资金净流入排名
func StockFundRankUpdate(pageIdx int64, tradeDate time.Time) (bStatus bool) {

	nSortID := pageIdx*100 + 1
	strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
	params := url.Values{}
	params.Add("np", "1")
	params.Add("fltt", "2")
	params.Add("invt", "2")
	params.Add("fs", "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2")
	params.Add("fields", "f2,f3,f7,f8,f10,f12,f14,f62,f184,f66,f69")
	params.Add("fid", "f62")                       // #净额:f62-今日  净占比:f184-今日
	params.Add("pn", fmt.Sprintf("%v", pageIdx+1)) // 翻页
	params.Add("pz", "100")                        // 大小
	params.Add("po", "1")
	params.Add("ut", "8dec03ba335b81bf4ebdf7b29ec27d15")
	params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "push2.eastmoney.com",
		"Referer":         "https://data.eastmoney.com/zjlx/detail.html",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[更新个股资金净流入排名] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[更新个股资金净流入排名] 操作[HttpGet] 状态码[%v]error:%s Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[更新个股资金净流入排名] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[更新个股资金净流入排名] 操作[data]  error:不存在")
		return
	}
	diff, ok := data["diff"].([]interface{})
	if !ok {
		logx.Errorf("[更新个股资金净流入排名] 操作[diff] error:不存在")
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
			logx.Errorf("[更新个股资金净流入排名] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		currentPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["f2"]))  //最新价
		increaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f3"]))  //涨幅
		turnoverRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f8"]))  //换手
		volumeRatio := decimal.NewFromFloat(cast.ToFloat64(itemMap["f10"]))  //量比
		mainFund := decimal.NewFromFloat(cast.ToFloat64(itemMap["f62"]))     //主力净流入(亿)
		mainPercent := decimal.NewFromFloat(cast.ToFloat64(itemMap["f184"])) //主力净流入占比
		superFund := decimal.NewFromFloat(cast.ToFloat64(itemMap["f66"]))    //超大单净流入(亿)
		superPercent := decimal.NewFromFloat(cast.ToFloat64(itemMap["f69"])) //超大单净流入占比

		sData := model.StockFundRank{
			StockCode:    stockCode,
			StockName:    stockName,
			PlateType:    rData.PlateType,
			MainFund:     mainFund.DivRound(decimal.NewFromInt(100000000), 2).InexactFloat64(),
			MainPercent:  mainPercent.InexactFloat64(),
			SuperFund:    superFund.DivRound(decimal.NewFromInt(100000000), 2).InexactFloat64(),
			SuperPercent: superPercent.InexactFloat64(),
			FundSortID:   nSortID, //净流入排名
			VolumeRatio:  volumeRatio.InexactFloat64(),
			TurnoverRate: turnoverRate.InexactFloat64(),
			IncreaseRate: increaseRate.InexactFloat64(),
			CurrentPrice: currentPrice.InexactFloat64(),
			TradingDate:  tradeDate,
			Industry:     rData.Industry,
			IndustryCode: rData.IndustryCode,
			UpdatedAt:    time.Now(),
		}

		err = dao.StockFundRank.Save(&sData)
		if err != nil {
			logx.Errorf("[更新个股资金净流入排名] [数据库]表[StockFundRank] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
			return
		}

		nSortID = nSortID + 1
	}

	if len(diff) == 100 {
		bStatus = true
	}

	return
}
