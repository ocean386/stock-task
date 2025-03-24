package task

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"net/url"
	"time"
)

// 概念板块-成份股票
func StockConceptBatchUpdate() {

	conceptSlice, err := dao.StockConcept.Find()
	if err != nil {
		logx.Errorf("[概念板块-成份股票] [数据库]表[StockDate] 操作[查询]-error:%s", err.Error())
		return
	}

	for _, conceptData := range conceptSlice {
		bStatus := true
		idx := int64(0)
		for bStatus {
			idx = idx + 1
			bStatus = StockConceptList(idx, conceptData.ConceptCode, conceptData.ConceptName)
		}
	}
}

func StockConceptList(idx int64, strConceptCode, strConceptName string) (bStatus bool) {

	strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
	params := url.Values{}
	params.Add("np", "1")
	params.Add("fltt", "1")
	params.Add("invt", "2")
	params.Add("fs", fmt.Sprintf("b:%v+f:!50", strConceptCode))
	params.Add("fields", "f12,f14")
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
		logx.Errorf("[概念板块-成份股票] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[概念板块-成份股票] 操作[HttpGet] 状态码[%v]", statusCode)
		return
	}

	// 解析响应数据
	var respData map[string]interface{}
	if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
		logx.Errorf("[概念板块-成份股票] 操作[JsonUnmarshal] error:%s", err.Error())
		return
	}

	// 处理数据
	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[概念板块-成份股票] 操作[data] 概念名称[%v] error:数据格式错误", strConceptName)
		return
	}

	diff, ok := data["diff"].([]interface{})
	if !ok {
		logx.Errorf("[概念板块-成份股票] 操作[diff] error:数据格式错误")
		return
	}

	// 保存数据到数据库
	for _, item := range diff {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode := cast.ToString(itemMap["f12"])
		stockName := cast.ToString(itemMap["f14"])

		// 检查是否已存在
		_, err = dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
		if err != nil {
			logx.Errorf("[概念板块-成份股票] [数据库]表[StockCode] 操作[查询]-error:%s", err.Error())
			continue
		}

		stock := model.StockConceptList{
			StockCode:   stockCode,
			StockName:   stockName, //股票名称
			ConceptCode: strConceptCode,
			ConceptName: strConceptName,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		if err := dao.StockConceptList.Save(&stock); err != nil {
			logx.Errorf("[概念板块-成份股票] [数据库]表[StockConceptList] 操作[保存] 概念代码[%v]-error:%v", strConceptCode, err)
			return
		}
	}

	if len(diff) == 100 {
		bStatus = true
	}

	logx.Infof("[概念板块-成份股票] 操作[完成] 概念名称[%v] 共处理[%v]条数据", strConceptName, len(diff))
	return
}
