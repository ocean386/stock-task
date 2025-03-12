package base

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/spf13/cast"
	"github.com/tealeg/xlsx"
	"github.com/zeromicro/go-zero/core/logx"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

/*
1.A股信息-更新: 股票代码 股票简称 交易所  市场类型 次新股 行业 行业代码
*/

// A股名称列表 stock_info_a_code_name
func StockTask() {

	//深圳A股(main-主板 nm-创业板)
	RequestHttpStockListSZ("main")
	time.Sleep(time.Second * 5)
	RequestHttpStockListSZ("nm")

	//上海A股(1-主板 8-科创板)
	for i := 1; i <= 8; i = i + 7 {
		RequestHttpStockListSH(i)
		time.Sleep(time.Second * 5)
	}

	//北京A股
	RequestHttpStockListBJ()

	//更新股票归属行业 以及行业代码(数据来源-东方财富)
	StockIndustryBatchUpdate()

	// 判断股票为次新股
	IsStockNew()

}

// 判断股票为次新股 stock_zt_pool_sub_new_em
func IsStockNew() {

	logx.Infof("执行 StockHTTP 任务:%v", time.Now().Format("15:04:05"))
	// 构建请求参数
	now := time.Now()
	params := url.Values{}
	params.Add("ut", "7eea3edcaed734bea9cbfc24409ed989")
	params.Add("dpt", "wz.ztzt")
	params.Add("Pageindex", "0")
	params.Add("pagesize", "200")
	params.Add("sort", "ods:asc")
	params.Add("date", now.Format("20060102")) // 当前日期
	params.Add("_", cast.ToString(now.UnixNano()/1e6))

	// 构建完整URL
	strUrl := "https://push2ex.eastmoney.com/getTopicCXPooll"
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "push2ex.eastmoney.com",
		"Referer":         "https://quote.eastmoney.com/ztb/detail",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}

	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("IsStockNew-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("IsStockNew-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Infof("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", strUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-StatusCode:%v", strUrl, statusCode)
		return
	}

	// 获取 data 中的 pool 列表
	poolData, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:pageHelp", strUrl)
		return
	}
	dataList, ok := poolData["pool"].([]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:data", strUrl)
		return
	}

	// 解析所需字段
	for _, item := range dataList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["c"].(string)
		info, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Updates(model.Stock{
			IsNewlyListed: 1,
			UpdatedAt:     time.Now().Unix(),
		})
		if err != nil {
			logx.Errorf("MySql Stock Update error:%s", err.Error())
			return
		}

		if info.RowsAffected < 1 {
			logx.Errorf("MySql Stock Update error: 更新无效")
			return
		}
	}

}

func StockIndustryBatchUpdate() {

	stockList, err := dao.Stock.Where(dao.Stock.Industry.Eq("")).Find()
	if err != nil {
		logx.Errorf("dao stock find db error:%s", err.Error())
		return
	}

	for _, s := range stockList {
		strSecID := ""
		if s.Exchange == 1 || s.Exchange == 3 {
			strSecID = fmt.Sprintf("0.%v", s.StockCode)
		} else if s.Exchange == 2 {
			strSecID = fmt.Sprintf("1.%v", s.StockCode)
		}
		if len(strSecID) == 0 {
			continue
		}
		RequestHttpStockIndustry(strSecID, s.StockCode)
		time.Sleep(time.Millisecond * 100)
	}
}

// 获取个股所属行业  stock_individual_info_em
func RequestHttpStockIndustry(strSecID, strStockCode string) {

	strUrl := "https://push2.eastmoney.com/api/qt/stock/get"
	params := url.Values{}
	params.Add("invt", "2")
	params.Add("fltt", "2")
	params.Add("fields", "f127,f198")
	params.Add("secid", strSecID)
	params.Add("ut", "fa5fd1943c7b386f172d6893dbfba10b")
	params.Add("_", fmt.Sprintf("%d", time.Now().Unix()))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("RequestHttp-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Infof("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", strUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-StatusCode:%v", strUrl, statusCode)
		return
	}

	// 获取 pageHelp 中的 data 列表
	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:pageHelp", strUrl)
		return
	}
	strIndustryName, ok1 := data["f127"].(string)
	strIndustryCode, ok2 := data["f198"].(string)

	if !ok1 || !ok2 {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:data", strUrl)
		return
	}

	info, err := dao.Stock.Where(dao.Stock.StockCode.Eq(strStockCode)).Updates(model.Stock{
		Industry:     strIndustryName,
		IndustryCode: strIndustryCode,
		UpdatedAt:    time.Now().Unix(),
	})
	if err != nil {
		logx.Errorf("MySql Stock Update error:%s", err.Error())
		return
	}

	if info.RowsAffected < 1 {
		logx.Errorf("MySql Stock Update error: 更新无效")
		return
	}

	logx.Infof("%v %v %v ", strSecID, strIndustryName, strIndustryCode)

}

// RequestHttpStockListSZ HTTP Get 请求 深圳交易所-股票列表
func RequestHttpStockListSZ(showType string) {

	strUrl := "https://www.szse.cn/api/report/ShowReport"
	nMarketType := 2
	strRange := "20%"
	if showType == "main" {
		nMarketType = 1
		strRange = "10%"
	}

	// 构建请求参数
	params := url.Values{}
	params.Add("SHOWTYPE", "xlsx")
	params.Add("CATALOGID", "1110")
	params.Add("TABKEY", "tab1")
	params.Add("selectModule", showType)
	params.Add("random", fmt.Sprintf("%.16f", rand.Float64()))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "www.szse.cn",
		"Referer":         "https://www.szse.cn/market/product/stock/list/index.html",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}

	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("RequestHttp-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	// 检查响应状态码
	if statusCode != 200 {
		logx.Errorf("RequestHttp-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析 Excel 文件
	xlFile, err := xlsx.OpenBinary(respBytes)
	if err != nil {
		logx.Errorf("RequestHttp,helper.Response-ExcelUnmarshal,[%s]-error:%s", strUrl, err.Error())
		return
	}

	// 假设数据在第一个工作表中
	if len(xlFile.Sheets) > 0 {
		sheet := xlFile.Sheets[0]
		for i, row := range sheet.Rows {
			// 跳过表头
			if i == 0 {
				continue
			}
			if len(row.Cells) >= 7 {
				stockCode := strings.TrimSpace(row.Cells[4].Value)
				stockName := strings.TrimSpace(row.Cells[5].Value)
				listDate := strings.TrimSpace(row.Cells[6].Value)
				listDate = strings.Replace(listDate, "-", "", 2)
				re := regexp.MustCompile(`<[^>]*>`)
				stockName = re.ReplaceAllString(stockName, "")
				if strings.Contains(stockName, "ST") {
					continue
				}

				data := model.Stock{
					StockCode:     stockCode,
					StockName:     stockName,
					Exchange:      1,                  //交易所(1-深圳,2-上海,3-北京)
					MarketType:    int64(nMarketType), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
					IncreaseRange: strRange,
					IsNewlyListed: 0,
					ListingDate:   cast.ToInt64(listDate),
					CreatedAt:     time.Now().Unix(),
					UpdatedAt:     time.Now().Unix(),
				}
				logx.Infof("Code:%v Name:%v Date:%v", stockCode, stockName, listDate)
				err = dao.Stock.Save(&data)
				if err != nil {
					logx.Errorf("MySql Stock Save error:%s", err.Error())
					return
				}
			}
		}
	}

}

// RequestHttpStockList HTTP Get 请求 上海交易所-股票列表
func RequestHttpStockListSH(stockType int) {

	strUrl := "https://query.sse.com.cn/sseQuery/commonQuery.do"
	nMarketType := 3
	strRange := "20%"
	if stockType == 1 {
		nMarketType = 1
		strRange = "10%"
	}

	params := url.Values{}
	params.Add("STOCK_TYPE", fmt.Sprintf("%v", stockType))
	params.Add("REG_PROVINCE", "")
	params.Add("CSRC_CODE", "")
	params.Add("STOCK_CODE", "")
	params.Add("sqlId", "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L")
	params.Add("COMPANY_STATUS", "2,4,5,7,8")
	params.Add("type", "inParams")
	params.Add("isPagination", "true")
	params.Add("pageHelp.cacheSize", "1")
	params.Add("pageHelp.beginPage", "1")
	params.Add("pageHelp.pageSize", "10000")
	params.Add("pageHelp.pageNo", "1")
	params.Add("pageHelp.endPage", "1")
	params.Add("_", fmt.Sprintf("%d", time.Now().UnixNano()/1e6))
	fullUrl := fmt.Sprintf("%s?%s", strUrl, params.Encode())

	// 设置请求头
	headers := map[string]string{
		"Accept":          "*/*",
		"Connection":      "keep-alive",
		"Accept-Language": "zh-CN,zh;q=0.9",
		"Host":            "query.sse.com.cn",
		"Referer":         "https://www.sse.com.cn/",
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}
	respBytes, statusCode, err := internalHttp.HttpGet(false, fullUrl, headers)
	if err != nil {
		logx.Errorf("RequestHttp-HttpGet,[%s]-error:%s", fullUrl, err.Error())
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-HttpGet,[%s]-statusCode:%v", fullUrl, statusCode)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Infof("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", strUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-StatusCode:%v", strUrl, statusCode)
		return
	}

	// 获取 pageHelp 中的 data 列表
	pageHelp, ok := respData["pageHelp"].(map[string]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:pageHelp", strUrl)
		return
	}
	dataList, ok := pageHelp["data"].([]interface{})
	if !ok {
		logx.Errorf("RequestHttp-http.StatusOK,[%s]-Response filed:data", strUrl)
		return
	}

	// 解析所需字段
	for _, item := range dataList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["A_STOCK_CODE"].(string)
		stockName, _ := itemMap["COMPANY_ABBR"].(string)
		listDate, _ := itemMap["LIST_DATE"].(string)
		if strings.Contains(stockName, "ST") {
			continue
		}

		data := model.Stock{
			StockCode:     stockCode,
			StockName:     stockName,
			Exchange:      2,                  //交易所(1-深圳,2-上海,3-北京)
			MarketType:    int64(nMarketType), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
			IncreaseRange: strRange,
			IsNewlyListed: 0,
			ListingDate:   cast.ToInt64(listDate),
			CreatedAt:     time.Now().Unix(),
			UpdatedAt:     time.Now().Unix(),
		}
		err = dao.Stock.Save(&data)
		if err != nil {
			logx.Errorf("MySql Stock Save error:%s", err.Error())
			return
		}
	}
}

// RequestHttpStockListBJ HTTP Post 请求 北京交易所-股票列表
func RequestHttpStockListBJ() {

	strUrl := "https://www.bse.cn/nqxxController/nqxxCnzq.do"
	// 第一次请求获取总页数
	params := url.Values{}
	params.Add("page", "0")
	params.Add("typejb", "T")
	params.Add("xxfcbj[]", "2")
	params.Add("xxzqdm", "")
	params.Add("sortfield", "xxzqdm")
	params.Add("sorttype", "asc")

	headers := map[string]string{
		"Accept":           "application/json,text/javascript,*/*; q=0.01",
		"Connection":       "keep-alive",
		"Accept-Language":  "zh-CN,zh;q=0.9",
		"Content-type":     "application/x-www-form-urlencoded; charset=UTF-8",
		"Host":             "www.bse.cn",
		"Referer":          "https://www.bse.cn/nq/listedcompany.html",
		"User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		"X-Requested-With": "XMLHttpRequest",
	}

	var respBytes []byte
	var statusCode int
	var err error
	currentUrl := strUrl
	respBytes, statusCode, err = internalHttp.HttpPost(false, currentUrl, []byte(params.Encode()), headers)
	if err != nil {
		logx.Errorf("RequestHttp-HttpPost,[%s]-error:%s", currentUrl, err.Error())
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("RequestHttp-HttpPost,[%s]-statusCode:%v", currentUrl, statusCode)
		return
	}

	strResp := strings.Replace(string(respBytes), "null([", "", 1)
	respBytes = []byte(strings.Replace(strResp, "])", "", 1))
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", currentUrl, err.Error())
		return
	}

	totalPages := 0
	if len(respData) > 0 {
		totalPagesFloat, ok := respData["totalPages"].(float64)
		if ok {
			totalPages = int(totalPagesFloat)
		}
	}

	// 循环请求所有页面
	for page := 0; page < totalPages; page++ {
		if page > 0 {
			params.Set("page", fmt.Sprintf("%d", page))

			respBytes, statusCode, err = internalHttp.HttpPost(false, currentUrl, []byte(params.Encode()), headers)
			if err != nil {
				logx.Errorf("RequestHttp-HttpPost,[%s]-error:%s", currentUrl, err.Error())
				return
			}

			if statusCode != http.StatusOK {
				logx.Errorf("RequestHttp-HttpPost,[%s]-statusCode:%v", currentUrl, statusCode)
				return
			}

			strResp = strings.Replace(string(respBytes), "null([", "", 1)
			respBytes = []byte(strings.Replace(strResp, "])", "", 1))
			err = internalHttp.JsonUnmarshal(respBytes, &respData)
			if err != nil {
				logx.Errorf("RequestHttp,helper.Response-JsonUnmarshal,[%s]-error:%s", currentUrl, err.Error())
				return
			}
		}

		if len(respData) > 0 {
			content, ok := respData["content"].([]interface{})
			if ok {
				for _, item := range content {
					itemMap, ok := item.(map[string]interface{})
					if ok {
						stockCode, _ := itemMap["xxzqdm"].(string)
						stockName, _ := itemMap["xxzqjc"].(string)
						listDate, _ := itemMap["fxssrq"].(string)
						if strings.Contains(stockName, "ST") {
							continue
						}

						data := model.Stock{
							StockCode:     stockCode,
							StockName:     stockName,
							Exchange:      3,        //交易所(1-深圳,2-上海,3-北京)
							MarketType:    int64(4), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
							IncreaseRange: "30%",
							IsNewlyListed: 0,
							ListingDate:   cast.ToInt64(listDate),
							CreatedAt:     time.Now().Unix(),
							UpdatedAt:     time.Now().Unix(),
						}
						err = dao.Stock.Save(&data)
						if err != nil {
							logx.Errorf("MySql Stock Save error:%s", err.Error())
							return
						}
					}
				}
			}
		}
		time.Sleep(time.Second * 5)
	}
}
