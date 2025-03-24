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

// 初始化概念列表-A股
func InitStockConceptList() {

	bStatus := true
	idx := 0
	for bStatus {
		idx = idx + 1
		strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
		params := url.Values{}
		params.Add("np", "1")
		params.Add("fltt", "1")
		params.Add("invt", "2")
		params.Add("fs", "m:90+t:3+f:!50")
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
			logx.Errorf("[初始化概念列表] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
			return
		}

		if statusCode != http.StatusOK {
			logx.Errorf("[初始化概念列表] 操作[HttpGet] 状态码[%v]", statusCode)
			return
		}

		// 解析响应数据
		var respData map[string]interface{}
		if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
			logx.Errorf("[初始化概念列表] 操作[JsonUnmarshal] error:%s", err.Error())
			return
		}

		// 处理数据
		data, ok := respData["data"].(map[string]interface{})
		if !ok {
			logx.Errorf("[初始化概念列表] 操作[data] error:数据格式错误")
			return
		}

		diff, ok := data["diff"].([]interface{})
		if !ok {
			logx.Errorf("[初始化概念列表] 操作[diff] error:数据格式错误")
			return
		}

		// 保存数据到数据库
		for _, item := range diff {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			industryCode := cast.ToString(itemMap["f12"])
			industryName := cast.ToString(itemMap["f14"])

			concept := model.StockConcept{
				ConceptCode:    industryCode,
				ConceptName:    industryName,
				IsWatchConcept: 0, // 默认不自选
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			if err := dao.StockConcept.Save(&concept); err != nil {
				logx.Errorf("[初始化概念列表] [数据库]表[StockIndustry] 操作[保存] 概念代码[%v]-error:%v", industryCode, err)
				continue
			}
		}

		if len(diff) < 100 {
			bStatus = false
		}

		logx.Infof("[初始化概念列表] 操作[完成] 共处理[%v]条数据", len(diff))
	}

}
