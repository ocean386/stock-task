package base

import (
	"fmt"
	internalHttp "github.com/ocean386/common/http"
	"github.com/ocean386/stock-task/internal/orm/dao"
	"github.com/ocean386/stock-task/internal/orm/model"
	"github.com/spf13/cast"
	"github.com/tealeg/xlsx"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// A股名称列表
func StockTask() {

	//深圳A股(main-主板 nm-创业板)
	GetStockListSZ("main", false)
	time.Sleep(time.Second * 5)
	GetStockListSZ("nm", false)

	//上海A股(1-主板 8-科创板)
	for i := 1; i <= 8; i = i + 7 {
		GetStockListSH(i, false)
		time.Sleep(time.Second * 5)
	}

	//北京A股
	GetStockListBJ(false)

	//更新股票归属行业 以及行业代码(数据来源-东方财富)
	StockIndustryBatchUpdate()

	// 判断股票为次新股
	IsStockNew()

	// 初始化行业列表
	InitStockIndustryList()

	// 初始化概念列表-A股
	InitStockConceptList()
}

// A股名称列表
func StockNameUpdate() {

	//深圳A股(main-主板 nm-创业板)
	GetStockListSZ("main", true)
	time.Sleep(time.Second * 5)
	GetStockListSZ("nm", true)

	//上海A股(1-主板 8-科创板)
	for i := 1; i <= 8; i = i + 7 {
		GetStockListSH(i, true)
		time.Sleep(time.Second * 5)
	}

	//北京A股
	GetStockListBJ(true)

	// 判断股票为次新股
	IsStockNew()
}

// 更新个股为次新股
func IsStockNew() {

	now := time.Now()
	params := url.Values{}
	params.Add("ut", "7eea3edcaed734bea9cbfc24409ed989")
	params.Add("dpt", "wz.ztzt")
	params.Add("Pageindex", "0")
	params.Add("pagesize", "200")
	params.Add("sort", "ods:asc")
	params.Add("date", now.Format("20060102")) // 当前日期
	params.Add("_", cast.ToString(now.UnixNano()/1e6))

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
		logx.Errorf("[更新个股为次新股] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[更新个股为次新股] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[更新个股为次新股] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	poolData, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[更新个股为次新股] 操作[data] error:不存在")
		return
	}
	dataList, ok := poolData["pool"].([]interface{})
	if !ok {
		logx.Errorf("[更新个股为次新股] 操作[pool] error:不存在")
		return
	}

	for _, item := range dataList {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		stockCode, _ := itemMap["c"].(string)
		info, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Updates(model.Stock{
			IsNewlyListed: 1,
			UpdatedAt:     time.Now(),
		})
		if err != nil {
			logx.Errorf("[更新个股为次新股] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
			return
		}

		if info.RowsAffected < 1 {
			logx.Errorf("[更新个股为次新股] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:更新无效", stockCode)
			return
		}
	}
}

func StockIndustryBatchUpdate() {

	stockList, err := dao.Stock.Where(dao.Stock.Industry.Eq("")).Find()
	if err != nil {
		logx.Errorf("[更新个股所属行业] [数据库]表[Stock] 操作[查询] error:%v", err)
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
		GetStockIndustry(strSecID, s.StockCode)
		time.Sleep(time.Millisecond * 100)
	}
}

// 更新个股所属行业
func GetStockIndustry(strSecID, strStockCode string) {

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
		logx.Errorf("[更新个股所属行业] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[更新个股所属行业] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[更新个股所属行业] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[更新个股所属行业] 操作[data] error:不存在")
		return
	}
	strIndustryName, ok1 := data["f127"].(string)
	strIndustryCode, ok2 := data["f198"].(string)

	if !ok1 || !ok2 {
		logx.Errorf("[更新个股所属行业] 操作[data] error:f127,f128不存在")
		return
	}

	info, err := dao.Stock.Where(dao.Stock.StockCode.Eq(strStockCode)).Updates(model.Stock{
		Industry:     strIndustryName,
		IndustryCode: strIndustryCode,
		UpdatedAt:    time.Now(),
	})
	if err != nil {
		logx.Errorf("[更新个股所属行业] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", strStockCode, err)
		return
	}

	if info.RowsAffected < 1 {
		logx.Errorf("[更新个股所属行业] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:更新无效", strStockCode)
		return
	}
}

// GetStockListSZ HTTP Get 请求 深圳交易所-股票列表
func GetStockListSZ(showType string, bUpdate bool) {

	strUrl := "https://www.szse.cn/api/report/ShowReport"
	nMarketType := 2
	fRange := float64(20)
	if showType == "main" {
		nMarketType = 1
		fRange = 10
	}

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
		logx.Errorf("[深圳交易所-股票列表] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != 200 {
		logx.Errorf("[深圳交易所-股票列表] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析 Excel 文件
	xlFile, err := xlsx.OpenBinary(respBytes)
	if err != nil {
		logx.Errorf("[深圳交易所-股票列表] 操作[xlsx.OpenBinary] error:%s Url地址[%s]", err.Error(), fullUrl)
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
				tDate, _ := time.Parse(time.DateOnly, listDate)
				re := regexp.MustCompile(`<[^>]*>`)
				stockName = re.ReplaceAllString(stockName, "")
				if strings.Contains(stockName, "ST") {
					continue
				}

				if bUpdate == false {
					data := model.Stock{
						StockCode:     stockCode,
						StockName:     stockName,
						Exchange:      1,                  //交易所(1-深圳,2-上海,3-北京)
						MarketType:    int64(nMarketType), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
						IncreaseRange: fRange,
						IsNewlyListed: 0,
						ListingDate:   tDate,
						CreatedAt:     time.Now(),
						UpdatedAt:     time.Now(),
					}

					err = dao.Stock.Save(&data)
					if err != nil {
						logx.Errorf("[深圳交易所-股票列表] [数据库]表[Stock] 操作[插入] 股票代码[%v]-error:%v", stockCode, err)
						return
					}
				} else {
					data, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
					if err != nil {
						logx.Errorf("[深圳交易所-股票列表] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
						return
					}

					if data.StockName != stockName {
						data.StockName = stockName
						data.UpdatedAt = time.Now()
						if strings.Contains(stockName, "ST") {
							data.IsStStock = 1
						}

						err = dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Save(data)
						if err != nil {
							logx.Errorf("[深圳交易所-股票列表] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
							return
						}
					}
				}
			}
		}
	}
}

// GetStockList HTTP Get 请求 上海交易所-股票列表
func GetStockListSH(stockType int, bUpdate bool) {

	strUrl := "https://query.sse.com.cn/sseQuery/commonQuery.do"
	nMarketType := 3
	fRange := float64(20)
	if stockType == 1 {
		nMarketType = 1
		fRange = 10
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
		logx.Errorf("[上海交易所-股票列表] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	// 检查响应状态码
	if statusCode != http.StatusOK {
		logx.Errorf("[上海交易所-股票列表] 操作[HttpGet] 状态码[%v] Url地址[%s]", statusCode, fullUrl)
		return
	}

	// 解析响应 JSON
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[上海交易所-股票列表] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	pageHelp, ok := respData["pageHelp"].(map[string]interface{})
	if !ok {
		logx.Errorf("[上海交易所-股票列表] 操作[pageHelp] error:不存在")
		return
	}
	dataList, ok := pageHelp["data"].([]interface{})
	if !ok {
		logx.Errorf("[上海交易所-股票列表] 操作[data] error:不存在")
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
		tDate, _ := time.Parse("20060102", listDate)
		if strings.Contains(stockName, "ST") {
			continue
		}

		if bUpdate == false {
			data := model.Stock{
				StockCode:     stockCode,
				StockName:     stockName,
				Exchange:      2,                  //交易所(1-深圳,2-上海,3-北京)
				MarketType:    int64(nMarketType), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
				IncreaseRange: fRange,
				IsNewlyListed: 0,
				ListingDate:   tDate,
				CreatedAt:     time.Now(),
				UpdatedAt:     time.Now(),
			}
			err = dao.Stock.Save(&data)
			if err != nil {
				logx.Errorf("[上海交易所-股票列表] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
				return
			}
		} else {
			data, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
			if err != nil {
				logx.Errorf("[上海交易所-股票列表] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
				return
			}

			if data.StockName != stockName {
				data.StockName = stockName
				data.UpdatedAt = time.Now()
				if strings.Contains(stockName, "ST") {
					data.IsStStock = 1
				}

				err = dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Save(data)
				if err != nil {
					logx.Errorf("[上海交易所-股票列表] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
					return
				}
			}
		}
	}
}

// GetStockListBJ HTTP Post 请求 北京交易所-股票列表
func GetStockListBJ(bUpdate bool) {

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
	respBytes, statusCode, err = internalHttp.HttpPost(false, strUrl, []byte(params.Encode()), headers)
	if err != nil {
		logx.Errorf("[北京交易所-股票列表] 操作[HttpPost] error:%s Url地址[%s]", err.Error(), strUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[北京交易所-股票列表] 操作[HttpPost] 状态码[%v] Url地址[%s]", statusCode, strUrl)
		return
	}

	strResp := strings.Replace(string(respBytes), "null([", "", 1)
	respBytes = []byte(strings.Replace(strResp, "])", "", 1))
	var respData map[string]interface{}
	err = internalHttp.JsonUnmarshal(respBytes, &respData)
	if err != nil {
		logx.Errorf("[北京交易所-股票列表] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), strUrl)
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

			respBytes, statusCode, err = internalHttp.HttpPost(false, strUrl, []byte(params.Encode()), headers)
			if err != nil {
				logx.Errorf("[北京交易所-股票列表] 操作[HttpPost] error:%s Url地址[%s]", err.Error(), strUrl)
				return
			}

			if statusCode != http.StatusOK {
				logx.Errorf("[北京交易所-股票列表] 操作[HttpPost] 状态码[%v] Url地址[%s]", statusCode, strUrl)
				return
			}

			strResp = strings.Replace(string(respBytes), "null([", "", 1)
			respBytes = []byte(strings.Replace(strResp, "])", "", 1))
			err = internalHttp.JsonUnmarshal(respBytes, &respData)
			if err != nil {
				logx.Errorf("[北京交易所-股票列表] 操作[JsonUnmarshal] error:%s Url地址[%s]", err.Error(), strUrl)
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
						tDate, _ := time.Parse("20060102", listDate)
						if strings.Contains(stockName, "ST") {
							continue
						}

						if bUpdate == false {
							data := model.Stock{
								StockCode:     stockCode,
								StockName:     stockName,
								Exchange:      3,        //交易所(1-深圳,2-上海,3-北京)
								MarketType:    int64(4), //市场类别(1-主板10%,2-创业板20%,3-科创板20%,4-北交所30%)
								IncreaseRange: 30,
								IsNewlyListed: 0,
								ListingDate:   tDate,
								CreatedAt:     time.Now(),
								UpdatedAt:     time.Now(),
							}
							err = dao.Stock.Save(&data)
							if err != nil {
								logx.Errorf("[北京交易所-股票列表] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
								return
							}
						} else {
							data, err := dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).First()
							if err != nil {
								logx.Errorf("[北京交易所-股票列表] [数据库]表[Stock] 操作[查询] 股票代码[%v]-error:%v", stockCode, err)
								return
							}

							if data.StockName != stockName {
								data.StockName = stockName
								data.UpdatedAt = time.Now()
								if strings.Contains(stockName, "ST") {
									data.IsStStock = 1
								}

								err = dao.Stock.Where(dao.Stock.StockCode.Eq(stockCode)).Save(data)
								if err != nil {
									logx.Errorf("[北京交易所-股票列表] [数据库]表[Stock] 操作[更新] 股票代码[%v]-error:%v", stockCode, err)
									return
								}
							}
						}
					}
				}
			}
		}
		time.Sleep(time.Second * 2)
	}
}

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

			conceptCode := cast.ToString(itemMap["f12"])
			conceptName := cast.ToString(itemMap["f14"])

			conceptResult, err := dao.StockConcept.Where(dao.StockConcept.ConceptCode.Eq(conceptCode)).First()
			if err != nil && err != gorm.ErrRecordNotFound {
				logx.Errorf("[初始化概念列表] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
				return
			}

			if conceptResult != nil {
				continue
			}

			concept := model.StockConcept{
				ConceptCode:    conceptCode,
				ConceptName:    conceptName,
				IsWatchConcept: 0, // 默认不自选
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			if err := dao.StockConcept.Save(&concept); err != nil {
				logx.Errorf("[初始化概念列表] [数据库]表[StockConcept] 操作[保存] 概念代码[%v]-error:%v", conceptCode, err)
				continue
			}
		}

		if len(diff) < 100 {
			bStatus = false
		}

		logx.Infof("[初始化概念列表] 操作[完成] 共处理[%v]条数据", len(diff))
	}
}

// 初始化行业列表-A股
func InitStockIndustryList() {
	strUrl := "https://push2.eastmoney.com/api/qt/clist/get"
	params := url.Values{}
	params.Add("np", "1")
	params.Add("fltt", "1")
	params.Add("invt", "2")
	params.Add("fs", "m:90+t:2+f:!50")
	params.Add("fields", "f12,f14")
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
		logx.Errorf("[初始化行业列表] 操作[HttpGet] error:%s Url地址[%s]", err.Error(), fullUrl)
		return
	}

	if statusCode != http.StatusOK {
		logx.Errorf("[初始化行业列表] 操作[HttpGet] 状态码[%v]", statusCode)
		return
	}

	// 解析响应数据
	var respData map[string]interface{}
	if err := internalHttp.JsonUnmarshal(respBytes, &respData); err != nil {
		logx.Errorf("[初始化行业列表] 操作[JsonUnmarshal] error:%s", err.Error())
		return
	}

	// 处理数据
	data, ok := respData["data"].(map[string]interface{})
	if !ok {
		logx.Errorf("[初始化行业列表] 操作[data] error:数据格式错误")
		return
	}

	diff, ok := data["diff"].([]interface{})
	if !ok {
		logx.Errorf("[初始化行业列表] 操作[diff] error:数据格式错误")
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

		industry := model.StockIndustry{
			IndustryCode:    industryCode,
			IndustryName:    industryName,
			IsWatchIndustry: 0, // 默认不自选
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}

		if err := dao.StockIndustry.Save(&industry); err != nil {
			logx.Errorf("[初始化行业列表] [数据库]表[StockIndustry] 操作[保存] 行业代码[%v]-error:%v", industryCode, err)
			continue
		}
	}

	logx.Infof("[初始化行业列表] 操作[完成] 共处理[%v]条数据", len(diff))
}
