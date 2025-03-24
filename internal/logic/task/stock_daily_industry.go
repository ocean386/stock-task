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

// 更新行业板块-每日领涨股票
func StockDailyIndustryUpdate() {

	strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
	params := url.Values{}
	params.Add("np", "1")
	params.Add("fltt", "1")
	params.Add("invt", "2")
	params.Add("fs", "m:90+t:2+f:!50")
	params.Add("fields", "f3,f12,f14,f104,f105,f128,f140,f136")
	params.Add("fid", "f3")
	params.Add("pn", "1")
	params.Add("pz", "200")
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
		logx.Errorf("[行业板块-每日领涨股票] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[行业板块-每日领涨股票] 操作[HttpGet] 状态码[%v]", statusCode)
		return
	}

	// 解析响应数据
	var respData map[string]interface{}
	if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
		logx.Errorf("[行业板块-每日领涨股票] 操作[JsonUnmarshal] error:%s", err.Error())
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[行业板块-每日领涨股票] 操作[data] error:数据格式错误")
		return
	}

	diff, ok := data["diff"].([]interface{})
	if !ok {
		logx.Errorf("[行业板块-每日领涨股票] 操作[diff] error:数据格式错误")
		return
	}

	tradeDate, err := dao.StockDate.Where(dao.StockDate.StockDate.Lte(time.Now())).Order(dao.StockDate.StockDate.Desc()).First()
	if err != nil {
		logx.Errorf("[行业板块-每日领涨股票] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	for i, item := range diff {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		industryCode := cast.ToString(itemMap["f12"])
		industryName := cast.ToString(itemMap["f14"])
		// 检查是否已存在
		_, err = dao.StockIndustry.Where(dao.StockIndustry.IndustryCode.Eq(industryCode)).First()
		if err != nil {
			logx.Errorf("[行业板块-每日领涨股票] [数据库]表[StockIndustry] 操作[查询]-error:%s", err.Error())
			return
		}

		fIndustryIncreaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f3"]))
		fIncreaseRate := decimal.NewFromFloat(cast.ToFloat64(itemMap["f136"]))

		industry := model.StockDailyIndustry{
			IndustryCode:         cast.ToString(itemMap["f12"]),
			IndustryName:         industryName,
			UpNumber:             cast.ToInt64(itemMap["f104"]),
			DownNumber:           cast.ToInt64(itemMap["f105"]),
			IndustryIncreaseRate: fIndustryIncreaseRate.DivRound(decimal.NewFromInt(100), 2).InexactFloat64(),
			IndustryRank:         int64(i + 1),
			StockCode:            cast.ToString(itemMap["f140"]),
			StockName:            cast.ToString(itemMap["f128"]), //股票名称
			IncreaseRate:         fIncreaseRate.DivRound(decimal.NewFromInt(100), 2).InexactFloat64(),
			TradingDate:          tradeDate.StockDate,
			UpdatedAt:            time.Now(),
		}

		if err = dao.StockDailyIndustry.Save(&industry); err != nil {
			logx.Errorf("[行业板块-每日领涨股票] [数据库]表[StockDailyIndustry] 操作[Save] 行业代码[%v]-error:%v", industryCode, err)
			return
		}
	}

	logx.Infof("[行业板块-每日领涨股票] 操作[完成] 共处理[%v]条数据", len(diff))
}
