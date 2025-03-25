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
	"time"
)

// 更新概念板块-每日领涨股票(每个交易日执行一次)
func StockDailyConcept() {

	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[概念板块-每日领涨股票] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	bStatus := true
	idx := 0
	nRank := 0
	for bStatus {
		idx = idx + 1
		strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
		params := url.Values{}
		params.Add("np", "1")
		params.Add("fltt", "1")
		params.Add("invt", "2")
		params.Add("fs", "m:90+t:3+f:!50")
		params.Add("fields", "f3,f12,f14,f104,f105,f128,f140,f136")
		params.Add("fid", "f3")
		params.Add("pn", fmt.Sprintf("%v", idx))
		params.Add("pz", "100")
		params.Add("po", "1")
		params.Add("ut", "fa5fd1943c7b386f172d6893dbfba10b")
		params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))

		fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

		headers := map[string]string{
			"Accept":          "*/*",
			"Connection":      "keep-alive",
			"Accept-Language": "zh-CN,zh;q=0.9",
			"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
		}

		// 发送HTTP请求
		respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
		if err != nil {
			logx.Errorf("[概念板块-每日领涨股票] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
			return
		}

		if statusCode != http.StatusOK {
			logx.Errorf("[概念板块-每日领涨股票] 操作[HttpGet] 状态码[%v]", statusCode)
			return
		}

		// 解析响应数据
		var respData map[string]interface{}
		if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
			logx.Errorf("[概念板块-每日领涨股票] 操作[JsonUnmarshal] error:%s", err.Error())
			return
		}

		// 处理数据
		data, ok := respData["data"].(map[string]interface{})
		if !ok {
			logx.Errorf("[概念板块-每日领涨股票] 操作[data] error:数据格式错误")
			return
		}

		diff, ok := data["diff"].([]interface{})
		if !ok {
			logx.Errorf("[概念板块-每日领涨股票] 操作[diff] error:数据格式错误")
			return
		}

		// 保存数据到数据库
		for _, item := range diff {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			nRank = nRank + 1
			conceptCode := cast.ToString(itemMap["f12"])
			conceptName := cast.ToString(itemMap["f14"])
			stockName := cast.ToString(itemMap["f128"])
			stockCode := cast.ToString(itemMap["f140"])

			// 检查是否已存在
			_, err = dao.StockConcept.Where(dao.StockConcept.ConceptCode.Eq(conceptCode)).First()
			if err != nil && err != gorm.ErrRecordNotFound {
				logx.Errorf("[概念板块-每日领涨股票] [数据库]表[StockIndustry] 操作[查询]-error:%s", err.Error())
				return
			}

			fConceptIncreaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f3"]))
			fIncreaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f136"]))

			concept := model.StockDailyConcept{
				ConceptCode:         conceptCode,
				ConceptName:         conceptName,
				UpNumber:            cast.ToInt64(itemMap["f104"]),
				DownNumber:          cast.ToInt64(itemMap["f105"]),
				ConceptIncreaseRate: fConceptIncreaseRate.DivRound(decimal.NewFromInt(100), 2).InexactFloat64(),
				ConceptRank:         int64(nRank),
				StockCode:           stockCode,
				StockName:           stockName, //股票名称
				IncreaseRate:        fIncreaseRate.DivRound(decimal.NewFromInt(100), 2).InexactFloat64(),
				TradingDate:         tradeDate.StockDate,
				UpdatedAt:           time.Now(),
			}

			if err := dao.StockDailyConcept.Save(&concept); err != nil {
				logx.Errorf("[概念板块-每日领涨股票] [数据库]表[StockIndustry] 操作[保存] 概念代码[%v]-error:%v", conceptCode, err)
				return
			}
		}

		if len(diff) < 100 {
			bStatus = false
		}

		logx.Infof("[概念板块-每日领涨股票] 操作[完成] 共处理[%v]条数据", len(diff))
	}

}
