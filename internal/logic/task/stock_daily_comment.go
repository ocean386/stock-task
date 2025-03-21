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

// 更新A股每日股评-批量
func StockDailyCommentBatchUpdate() {

	var (
		bStatus bool
		idx     int
	)
	bStatus = true
	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[每日股评-A股] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	for bStatus == true {
		idx = idx + 1
		bStatus = StockDailyCommentUpdate(idx, tradeDate.StockDate)
		time.Sleep(time.Millisecond * 100)
	}

	logx.Infof("[每日股评-A股] 操作[更新] 状态[完成].")
}

// 更新每日股评
func StockDailyCommentUpdate(idx int, tradeDate time.Time) (bStatus bool) {

	strUrl := "https://datacenter-web.eastmoney.com/api/data/v1/get"
	params := url.Values{}

	params.Add("sortColumns", "ORG_PARTICIPATE")     //机构参与度 降序排名
	params.Add("sortTypes", "-1")                    // 降序排名
	params.Add("pageNumber", fmt.Sprintf("%v", idx)) //翻页
	params.Add("pageSize", "500")
	params.Add("reportName", "RPT_DMSK_TS_STOCKNEW")
	params.Add("columns", "SECURITY_CODE,CLOSE_PRICE,CHANGE_RATE,TURNOVERRATE,PRIME_COST,PRIME_COST_20DAYS,PRIME_COST_60DAYS,ORG_PARTICIPATE,TOTALSCORE,RANK,FOCUS")
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "datacenter-web.eastmoney.com",
		"Referer":         "https://data.eastmoney.com/stockcomment/",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[每日股评-A股] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[每日股评-A股] 操作[HttpGet] 状态码[%v]error:%s Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[每日股评-A股] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	result, ok := respData["result"].(map[string]interface{})
	if !ok {
		logx.Errorf("[每日股评-A股] 操作[data]  error:不存在")
		return
	}
	dataSlice, ok := result["data"].([]interface{})
	if !ok {
		logx.Errorf("[每日股评-A股] 操作[diff] error:不存在")
		return
	}

	// 解析所需字段并更新到数据库
	for _, item := range dataSlice {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode := cast.ToString(itemMap["SECURITY_CODE"]) //股票代码

		//检查Stock表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[每日股评-A股] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		currentPrice := decimal.NewFromFloat(cast.ToFloat64(itemMap["CLOSE_PRICE"]))       //最新价
		increaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["CHANGE_RATE"]))       //涨幅
		turnoverRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["TURNOVERRATE"]))      //换手
		primeCost := decimal.NewFromFloat(cast.ToFloat64(itemMap["PRIME_COST"]))           //主力成本-今日
		primeCost20 := decimal.NewFromFloat(cast.ToFloat64(itemMap["PRIME_COST_20DAYS"]))  //主力成本-20日
		primeCost60 := decimal.NewFromFloat(cast.ToFloat64(itemMap["PRIME_COST_60DAYS"]))  //主力成本-60日
		orgParticipate := decimal.NewFromFloat(cast.ToFloat64(itemMap["ORG_PARTICIPATE"])) //机构参与度
		score := decimal.NewFromFloat(cast.ToFloat64(itemMap["TOTALSCORE"]))               //综合得分
		rank := cast.ToInt64(itemMap["RANK"])                                              //综合得分-排名
		focus := decimal.NewFromFloat(cast.ToFloat64(itemMap["FOCUS"]))                    //关注指数

		mData := model.StockDailyComment{
			StockCode:      stockCode,
			StockName:      rData.StockName,
			PlateType:      rData.PlateType,
			TurnoverRate:   turnoverRate.RoundUp(2).InexactFloat64(),
			IncreaseRate:   increaseRate.RoundUp(2).InexactFloat64(),
			CurrentPrice:   currentPrice.Round(2).InexactFloat64(),
			PrimeCost:      primeCost.Round(2).InexactFloat64(),
			PrimeCost20:    primeCost20.Round(2).InexactFloat64(),
			PrimeCost60:    primeCost60.Round(2).InexactFloat64(),
			OrgParticipate: orgParticipate.RoundUp(2).Mul(decimal.NewFromInt(100)).InexactFloat64(),
			Score:          score.RoundUp(2).InexactFloat64(),
			Rank:           rank,
			Focus:          focus.RoundUp(2).InexactFloat64(),
			TradingDate:    tradeDate,
		}
		err = dao.StockDailyComment.Save(&mData)
		if err != nil {
			logx.Errorf("[每日股评-A股] [数据库]表[StockDailyComment] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
			return
		}
	}

	if len(dataSlice) == 500 {
		bStatus = true
	}

	return
}
