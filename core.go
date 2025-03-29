package main

import (
	"flag"
	"github.com/dcron-contrib/redisdriver"
	"github.com/libi/dcron"
	"github.com/ocean386/stock-task/internal/config"
	"github.com/ocean386/stock-task/internal/handler"
	"github.com/ocean386/stock-task/internal/logic/base"
	"github.com/ocean386/stock-task/internal/logic/task"
	"github.com/ocean386/stock-task/internal/nacos"
	"github.com/ocean386/stock-task/internal/svc"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"time"
)

var (
	nacosDataId = "stocktask" // 配置 Data ID
	nacosGroup  = "DEV"       // 配置组名
)

// 服务启动参数[编辑配置-环境]: NACOS_USERNAME=nacos;NACOS_PASSWORD=OKNacos;NACOS_IP=127.0.0.1
func main() {
	flag.Parse()

	// 加载yaml配置信息-naocs
	strNacosCfg, err := nacos.GetConfigFromNacos(nacosDataId, nacosGroup)
	if err != nil {
		logx.Errorf("Failed to get config from Nacos: %v", err)
		return
	}

	// 将 Nacos 配置内容加载到结构体中
	var cfg config.Config
	if err = conf.LoadFromYamlBytes([]byte(strNacosCfg), &cfg); err != nil {
		logx.Errorf("Failed to load config: %v", err)
		return
	}

	server := rest.MustNewServer(cfg.RestConf, rest.WithCors("*"))
	defer server.Stop()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisConf.Host,
		Password: cfg.RedisConf.Pass,
	})

	svcCtx := svc.NewServiceContext(cfg)
	redisDriver := redisdriver.NewDriver(redisClient)
	dCron := dcron.NewDcronWithOption("DCronServer", redisDriver,
		dcron.WithHashReplicas(10),
		dcron.WithNodeUpdateDuration(time.Second*10),
		dcron.CronOptionSeconds(),
	)

	// 每月15号更新下个月交易日期
	dCron.AddFunc("StockDateUpdate", "00 00 04 15 * *", func() {
		task.StockDateUpdate()
	})

	// 历史-月K线行情数据(每月1号更新一次)
	dCron.AddFunc("StockMonthMarketUpdate", "00 10 03 01 * *", func() {
		task.StockDailyMarketBatchUpdate(2)
	})

	// 历史-周K线行情数据(每周六更新一次)
	dCron.AddFunc("StockWeekMarketUpdate", "00 00 02 * * *", func() {
		WithTradeTimeCheck("StockWeekMarketUpdate", func() {
			task.StockDailyMarketBatchUpdate(1)
		})
	})

	// 更新概念的成份股列表(每周六更新一次)
	dCron.AddFunc("StockConceptListUpdate", "00 30 00 * * *", func() {
		WithTradeTimeCheck("StockConceptListUpdate", func() {
			task.StockConceptListBatchUpdate()
		})
	})

	// 更新-股票名称等信息(每周六更新一次)
	dCron.AddFunc("StockNameUpdate", "00 20 00 * * *", func() {
		WithTradeTimeCheck("StockNameUpdate", func() {
			base.StockNameUpdate()
		})
	})

	// 历史-日K线行情数据
	//dCron.AddFunc("StockDailyMarketUpdate", "", func() {
	//	task.StockDailyMarketBatchUpdate(0)
	//})

	// 每日龙虎榜(交易日下午5点钟更新一次)
	dCron.AddFunc("StockTigerLeaderUpdate", "00 00 17 * * *",
		WithTradeTimeCheck("StockTigerLeaderUpdate", func() {
			task.StockTigerLeaderBatchUpdate(svcCtx.SnowFlakeWorker)
		}),
	)

	// 实时行情数据(每5分钟更新一次)
	dCron.AddFunc("StockRealTimeMarketDataUpdate", "05 */5 * * * *",
		WithTradeTimeCheck("StockRealTimeMarketDataUpdate", func() {
			task.StockRealTimeMarketDataBatchUpdate(svcCtx.Redis, 1) // 0-流通市值 1-实时行情数据
		}),
	)

	// 每日强势榜(每5分钟更新一次)
	dCron.AddFunc("StockStrongPoolUpdate", "10 */5 * * * *",
		WithTradeTimeCheck("StockStrongPoolUpdate", func() {
			task.StockStrongPoolBatchUpdate(svcCtx.Redis)
		}),
	)

	// 每日个股异动(每5分钟更新一次)
	dCron.AddFunc("OrderChangeUpdate", "20 */5 * * * *",
		WithTradeTimeCheck("OrderChangeUpdate", func() {
			task.OrderChangeBatchUpdate(svcCtx.Redis)
		}),
	)

	// 每日人气榜(每5分钟更新一次)
	dCron.AddFunc("StockHotRankUpdate", "30 */5 * * * *",
		WithTradeTimeCheck("StockHotRankUpdate", func() {
			task.StockHotRankUpdate(svcCtx.Redis)
		}),
	)

	// 每日资金流向排名(每小时更新一次)
	dCron.AddFunc("StockFundRankUpdate", "00 35 * * * *",
		WithTradeTimeCheck("StockFundRankUpdate", task.StockFundRankBatchUpdate),
	)

	// 每日行业-领涨股票(每小时更新一次)
	dCron.AddFunc("StockDailyIndustryUpdate", "10 35 * * * *",
		WithTradeTimeCheck("StockDailyIndustryUpdate", task.StockDailyIndustryUpdate),
	)

	// 每日概念-领涨股票(每小时更新一次)
	dCron.AddFunc("StockDailyConcept", "15 35 * * * *",
		WithTradeTimeCheck("StockDailyConcept", task.StockDailyConcept),
	)

	// 每日股评(每小时更新一次)
	dCron.AddFunc("StockDailyCommentUpdate", "20 35 * * * *",
		WithTradeTimeCheck("StockDailyCommentUpdate", func() {
			task.StockDailyCommentBatchUpdate(svcCtx.Redis)
		}),
	)

	go dCron.Start()
	handler.RegisterHandlers(server, svcCtx)

	logx.Infof("Starting API Server [%s:%d]", cfg.Host, cfg.Port)
	server.Start()
}

func WithTradeTimeCheck(jobName string, f func()) func() {
	return func() {
		if IsTradeTime(jobName) {
			f()
		} else {
			logx.Infof("跳过非交易时间任务：%s", jobName)
		}
	}
}

// 交易时间判断
func IsTradeTime(strJobName string) bool {

	now := time.Now()

	if strJobName == "StockWeekMarketUpdate" || // 周六
		strJobName == "StockConceptListUpdate" ||
		strJobName == "StockNameUpdate" { // 周六
		if now.Weekday() == time.Saturday {
			return true
		}
		return false
	} else if strJobName == "StockMonthMarketUpdate" { // 每月1号
		if now.Day() == 1 {
			return true
		}
		return false
	} else { // 跳过周末
		if now.Weekday() >= time.Saturday || now.Weekday() <= time.Sunday {
			return false
		}
	}

	// 定义交易时间段
	startAM := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, now.Location())
	endAM := time.Date(now.Year(), now.Month(), now.Day(), 11, 30, 55, 0, now.Location())
	startPM := time.Date(now.Year(), now.Month(), now.Day(), 13, 0, 0, 0, now.Location())
	endPM := time.Date(now.Year(), now.Month(), now.Day(), 15, 0, 55, 0, now.Location())

	inAM := now.After(startAM) && now.Before(endAM)
	inPM := now.After(startPM) && now.Before(endPM)

	// 收盘后5分钟 1小时
	endPM5Min := endPM.Add(5 * time.Minute)
	endPM1Hour := endPM.Add(1 * time.Hour)
	afterEndPM := now.After(endPM)

	switch strJobName {
	// 实时数据任务
	case "StockRealTimeMarketDataUpdate":
		return inAM || inPM || (afterEndPM && (now.Before(endPM5Min) || now.Before(endPM1Hour)))
	// 盘中更新任务
	case "StockFundRankUpdate",
		"OrderChangeUpdate", "StockHotRankUpdate",
		"StockDailyCommentUpdate", "StockStrongPoolUpdate",
		"StockDailyIndustryUpdate", "StockDailyConcept":
		return inAM || inPM || (afterEndPM && (now.Before(endPM5Min) || now.Before(endPM1Hour)))
	// 盘后更新任务(龙虎榜)
	case "StockTigerLeaderUpdate":
		return now.After(endPM)
	default:
		return false
	}
}
