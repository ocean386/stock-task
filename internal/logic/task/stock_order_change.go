package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/ocean386/stock-task/internal/svc"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 更新盘口异动信息(每个交易日执行一次)
func OrderChangeBatchUpdate(svcCtx *svc.ServiceContext) {

	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[更新盘口异动] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	var stockCodeMap map[string]map[string]string
	stockCodeMap = make(map[string]map[string]string)

	strUrl := "https://push2ex.eastmoney.com/getAllStockChanges"
	typeSlice := []int{8201, 8202, 8193, 4, 32, 64, 8207, 8209, 8211, 8213, 8215, 8204, 8203, 8194, 8, 16, 128, 8208, 8210, 8212, 8214, 8216}
	for _, nType := range typeSlice {

		params := url.Values{}
		params.Add("type", fmt.Sprintf("%v", nType))
		params.Add("ut", "7eea3edcaed734bea9cbfc24409ed989")
		params.Add("pageindex", "0")
		params.Add("dpt", "wzchanges")
		params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
		fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

		// 设置请求头
		headers := map[string]string{
			"Accept":          "*/*",
			"Connection":      "keep-alive",
			"Accept-Language": "zh-CN,zh;q=0.9",
			"Host":            "push2ex.eastmoney.com",
			"Referer":         "https://quote.eastmoney.com/changes/?from=center",
			"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
		}

		respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
		if err != nil {
			logx.Errorf("[更新盘口异动] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
			return
		}

		if statusCode != http.StatusOK {
			logx.Errorf("[更新盘口异动] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
			return
		}

		var respData map[string]interface{}
		err = internalHttp.JsonUnmarshal(respBytes, &respData)
		if err != nil {
			logx.Errorf("[更新盘口异动] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
			return
		}

		dateStock, ok := respData["data"].(map[string]interface{})
		if !ok {
			logx.Errorf("[更新盘口异动] 操作[data] 类型[%v] error:不存在", nType)
			continue
		}

		dateList, ok := dateStock["allstock"].([]interface{})
		if !ok {
			logx.Errorf("[更新盘口异动] 操作[data] error:不存在")
			return
		}

		for _, item := range dateList {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			var (
				timeFormat string
				strMsg     string
			)
			timeData := cast.ToString(itemMap["tm"]) //时间
			stockCode := cast.ToString(itemMap["c"]) //股票代码
			orderType := cast.ToString(itemMap["t"]) //盘口异动类型

			if len(orderType) == 0 {
				logx.Errorf("[更新盘口异动] 操作[盘口异动类型] error:不存在")
				continue
			}

			strMsg = GetOrderChangeTypeMsg(orderType)

			//检查Stock表中是否存在该股票代码
			rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
			if err != nil || rData == nil {
				logx.Errorf("[更新盘口异动] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
				continue
			}

			if len(timeData) != 6 {
				timeData = "0" + timeData
			}
			timeFormat = timeData[0:2] + ":" + timeData[2:4] + ":" + timeData[4:6]

			if stockCodeMap[stockCode] == nil {
				stockCodeMap[stockCode] = make(map[string]string)
			}

			//stockCodeMap[stockCode][timeFormat] = fmt.Sprintf("%s-%s[股价:%.2f]", strMsg, timeFormat, cast.ToFloat64(infoSlice[1]))
			stockCodeMap[stockCode][timeFormat] = fmt.Sprintf("%s-%s", strMsg, timeFormat)
		}

		time.Sleep(time.Second * 2)
	}

	for stockCode, m := range stockCodeMap {
		var (
			strDataSlice []string
			strText      string
			nTimes       int64
		)
		for _, s := range m {
			strDataSlice = append(strDataSlice, s)
		}

		nTimes = int64(len(m))
		strText = strings.Join(strDataSlice, ",")

		//检查Stock表中是否存在该股票代码
		rData, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil || rData == nil {
			logx.Errorf("[更新盘口异动] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			return
		}

		orderData := &model.StockOrderChange{
			StockCode:    stockCode,
			StockName:    rData.StockName,
			PlateType:    rData.PlateType,
			ChangeTimes:  nTimes,
			ChangeMsg:    strText,
			TradingDate:  tradeDate.StockDate,
			Industry:     rData.Industry,
			IndustryCode: rData.IndustryCode,
			UpdatedAt:    time.Now(),
		}
		//err = dao.StockOrderChange.Where(dao.StockOrderChange.StockCode.Eq(stockCode), dao.StockOrderChange.TradingDate.Eq(tradeDate.StockDate)).Save(orderData)
		err = dao.StockOrderChange.Save(orderData)
		if err != nil {
			logx.Errorf("[更新盘口异动] [数据库]表[StockOrderChange] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
			return
		}
	}
}

func GetOrderChangeTypeMsg(strType string) (strMsg string) {

	OrderTypeMap := map[string]string{
		"8201": "火箭发射",
		"8202": "快速反弹",
		"8193": "大笔买入",
		"4":    "封涨停板",
		"32":   "打开跌停板",
		"64":   "有大买盘",
		"8207": "竞价上涨",
		"8209": "高开5日线",
		"8211": "向上缺口",
		"8213": "60日新高",
		"8215": "60日大幅上涨",
		"8204": "加速下跌",
		"8203": "高台跳水",
		"8194": "大笔卖出",
		"8":    "封跌停板",
		"16":   "打开涨停板",
		"128":  "有大卖盘",
		"8208": "竞价下跌",
		"8210": "低开5日线",
		"8212": "向下缺口",
		"8214": "60日新低",
		"8216": "60日大幅下跌",
	}

	msg, ok := OrderTypeMap[strType]
	if !ok {
		logx.Errorf("[更新盘口异动] 异动类型[%v] error:不存在", strType)
		return msg
	}

	return msg
}
