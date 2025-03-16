package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/common/snowflake"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/shopspring/decimal"
	"github.com/zeromicro/go-zero/core/logx"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 更新龙虎榜列表-批量
func StockTigerLeaderBatchUpdate(SnowFlakeWorker *snowflake.SnowFlakeIdWorker) {

	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[更新个股资金流排名] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	nType := int64(0) // 0-游资榜 1-机构榜
	for nType < 2 {
		StockTigerLeaderUpdate(nType, tradeDate.StockDate, SnowFlakeWorker) //净额
		nType = nType + 1
		time.Sleep(time.Second * 5)
	}

	StockTigerLeaderRebuild(tradeDate.StockDate)
	logx.Infof("[更新个股资金流排名] 操作[更新] 状态[完成].")
}

func StockTigerLeaderRebuild(tradeDate time.Time) {

	tigerSlice, err := dao.StockTigerLeader.Where(dao.StockTigerLeader.TradingDate.Eq(tradeDate)).Find()
	if err != nil {
		logx.Errorf("[更新龙虎榜] [数据库]表[StockTigerLeader] 操作[查询]-error:%s", err.Error())
		return
	}

	var tigerOrgSlice []*model.StockTigerLeader
	for _, t := range tigerSlice {
		if t.IsOrg == 1 && t.IsHotMoney == 0 {
			tigerOrgSlice = append(tigerOrgSlice, t)
		}
	}

	for _, t := range tigerSlice {
		if t.IsHotMoney == 0 {
			continue
		}
		for _, g := range tigerOrgSlice {
			if t.StockCode == g.StockCode {
				t.IsOrg = 1
				t.OrgTlabel = g.OrgTlabel
				info, err := dao.StockTigerLeader.Where(dao.StockTigerLeader.StockCode.Eq(g.StockCode), dao.StockTigerLeader.IsOrg.Eq(1), dao.StockTigerLeader.IsHotMoney.Eq(0)).Delete()
				if err != nil {
					logx.Errorf("[更新龙虎榜] [数据库]表[StockTigerLeader] 操作[删除] 股票代码[%v]-error:%v", t.StockCode, err)
					return
				}
				if info.RowsAffected < 1 {
					logx.Errorf("[更新龙虎榜] [数据库]表[StockTigerLeader] 操作[更新] 股票代码[%v]-error:更新无效", t.StockCode)
					continue
				}

				err = dao.StockTigerLeader.Save(t)
				if err != nil {
					logx.Errorf("[更新龙虎榜] [数据库]表[StockTigerLeader] 操作[更新] 股票代码[%v]-error:%v", t.StockCode, err)
					return
				}
			}
		}
	}
}

func StockTigerLeaderUpdate(nType int64, tradeDate time.Time, SnowFlakeWorker *snowflake.SnowFlakeIdWorker) {

	strUrl := "https://datacenter.eastmoney.com/securities/api/data/v1/get"
	params := url.Values{}
	if nType == 0 {
		params.Add("reportName", "RPT_BILLBOARD_HOTMONEY_DATELIST")
		params.Add("filter", fmt.Sprintf("(TRADE_DATE='%v')", tradeDate.Format(time.DateOnly)))
		params.Add("columns", "HOTMONEY_NAME,TLABEL,SECURITY_CODE,CHANGE_RATE,NET_AMT")
	} else {
		params.Add("reportName", "RPT_BILLBOARD_TRADEDAILY")
		params.Add("filter", fmt.Sprintf("(TRADE_DATE='%v')((ORG_BUY_TIMES > 0)(ORG_SELL_TIMES > 0 ))", tradeDate.Format(time.DateOnly)))
		params.Add("columns", "SECURITY_CODE,ORG_BUY_TIMES,ORG_SELL_TIMES,CHANGE_RATE,ORG_NET_BUY,TLABEL")
	}

	params.Add("source", "SECURITIES")
	params.Add("client", "APP")
	params.Add("pageNumber", "1")
	params.Add("pageSize", "200") // 大小
	if nType == 0 {
		params.Add("sortTypes", "-1,-1")
		params.Add("sortColumns", "HOTMONEY_NET_AMT,NET_AMT")
	} else {
		params.Add("sortTypes", "-1")
		params.Add("sortColumns", "ORG_NET_BUY")
	}
	params.Add("v", genRandomData())
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "datacenter.eastmoney.com",
		"Referer":         "https://emdata.eastmoney.com",
		"User-Agent":      "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("[更新龙虎榜] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[更新龙虎榜] 操作[HttpGet] 状态码[%v]error:%s Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[更新龙虎榜] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	resultData, ok := respData["result"].(map[string]interface{})
	if !ok {
		logx.Errorf("[更新龙虎榜] 操作[pageHelp] error:不存在")
		return
	}

	dataSlice, ok := resultData["data"].([]interface{})
	if !ok {
		logx.Errorf("[更新龙虎榜] 操作[data]  error:不存在")
		return
	}

	// 解析所需字段并更新到数据库
	for _, item := range dataSlice {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		var (
			hotName string
		)
		stockCode, _ := itemMap["SECURITY_CODE"].(string)   //股票代码
		increaseRate, _ := itemMap["CHANGE_RATE"].(float64) //涨跌幅度
		strTlabel, _ := itemMap["TLABEL"].(string)          //游资标签
		strTlabel = strings.Replace(strTlabel, "；", ",", -1)

		if nType == 0 {
			hotName, _ = itemMap["HOTMONEY_NAME"].(string) //游资名称
			//Amount, _ := itemMap["NET_AMT"].(string)     //主力净买入金额
		} else {
			//orgBuyTimes, _ := itemMap["ORG_BUY_TIMES"].(string) //买-机构数
			//orgSellTimes, _ := itemMap["ORG_SELL_TIMES"].(string) //卖-机构数
			//Amount, _ := itemMap["ORG_NET_BUY"].(string)     //机构净买入金额
		}

		// 检查 Stock 表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[更新龙虎榜] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		tData := model.StockTigerLeader{
			ID:                     SnowFlakeWorker.GenerateSnowFlakeID(),
			StockCode:              stockCode,
			StockName:              rData.StockName,
			CirculatingMarketValue: rData.CirculatingMarketValue,
			PlateType:              rData.PlateType,
			IncreaseRate:           decimal.NewFromFloat(increaseRate).Round(2).InexactFloat64(), //涨幅
			TradingDate:            tradeDate,
			Industry:               rData.Industry,
			IndustryCode:           rData.IndustryCode,
			UpdatedAt:              time.Now(),
		}

		if nType == 0 {
			tData.IsHotMoney = 1
			tData.HotTlabel = strTlabel
			tData.HotMoneyName = hotName
		} else {
			tData.IsOrg = 1
			tData.OrgTlabel = strTlabel
		}

		err = dao.StockTigerLeader.Save(&tData)
		if err != nil {
			logx.Errorf("[更新龙虎榜] [数据库]表[StockTigerLeader] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
			return
		}
	}
}

func genRandomData() string {

	var sb strings.Builder
	r := rand.New(rand.NewSource(0))
	sb.Grow(16)
	for i := 0; i < 16; i++ {
		sb.WriteByte(byte(r.Intn(10) + '0'))
	}
	return sb.String()
}
