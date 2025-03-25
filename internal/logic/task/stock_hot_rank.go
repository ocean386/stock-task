package task

import (
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"strings"
	"time"
)

// 更新个股人气榜-100名(每个交易日执行一次)
func StockHotRankUpdate() {

	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[更新个股人气榜] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	strUrl := "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Content-Type":    "application/json",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "gbcdn.dfcfw.com",
		"Referer":         "https://guba.eastmoney.com/",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
	}

	var (
		respBytes  []byte
		statusCode int
	)

	// 构建请求参数
	reqBody := map[string]interface{}{
		"appId":      "appId01",
		"globalId":   "786e4c21-70dc-435a-93bb-38",
		"marketType": "",
		"pageNo":     1,
		"pageSize":   100,
	}
	jsonBody, _ := internalHttp.JsonMarshal(reqBody)

	respBytes, statusCode, err = internalHttp.HttpPost(false, strUrl, jsonBody, headers)
	if err != nil {
		logx.Errorf("[更新个股人气榜] 操作[HttpPost] error:%s Url地址[%s]", err.Error(), strUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[更新个股人气榜] 操作[HttpPost] 状态码[%v] Url地址[%s]", statusCode, strUrl)
		return
	}

	// 解析响应JSON
	var respData map[string]interface{}
	if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
		logx.Errorf("[更新个股人气榜] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), strUrl)
		return
	}

	dataList, ok := respData["data"].([]interface{})
	if !ok {
		logx.Errorf("[更新个股人气榜] 操作[data] error:不存在")
		return
	}

	// 遍历处理数据
	for _, item := range dataList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["sc"].(string)
		fRank, _ := itemMap["rk"].(float64)
		fhRank, _ := itemMap["hisRc"].(float64)

		if strings.Contains(stockCode, "SZ") {
			stockCode = strings.Replace(stockCode, "SZ", "", 1)
		}
		if strings.Contains(stockCode, "SH") {
			stockCode = strings.Replace(stockCode, "SH", "", 1)
		}

		// 检查 Stock 表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[更新个股人气榜] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}

		rankData := model.StockHotRank{
			StockCode:       stockCode,
			StockName:       rData.StockName,
			PlateType:       rData.PlateType,
			HotSortID:       cast.ToInt64(fRank),
			YesterdaySortID: cast.ToInt64(fhRank),
			TradingDate:     tradeDate.StockDate,
			Industry:        rData.Industry,
			IndustryCode:    rData.IndustryCode,
			UpdatedAt:       time.Now(),
		}

		if err := dao.StockHotRank.Save(&rankData); err != nil {
			logx.Errorf("[更新个股人气榜] [数据库]表[StockHotRank] 操作[插入] 股票代码[%v]-error:%v", stockCode, err)
			continue
		}
	}

	logx.Info("[更新个股人气榜] [数据库]表[StockHotRank] 操作[插入] 更新完毕")
}
