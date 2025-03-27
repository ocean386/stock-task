package main

import (
	"flag"
	"github.com/dcron-contrib/redisdriver"
	"github.com/libi/dcron"
	"github.com/ocean386/stock-task/internal/config"
	"github.com/ocean386/stock-task/internal/handler"
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

	//每月15号更新下个月交易日期
	dCron.AddFunc("StockDateUpdate", "00 00 04 15 * *", func() {
		task.StockDateUpdate()
	})
	// 更新概念的成份股列表(每周六更新一次)
	dCron.AddFunc("", "00 10 00 * * *", func() {
		task.StockConceptListBatchUpdate()
	})
	//实时行情数据(每5分钟更新一次)
	dCron.AddFunc("StockRealTimeMarketDataUpdate", "*/5 * * * * *", func() {
		task.StockRealTimeMarketDataBatchUpdate(1) // 0-流通市值 1-实时行情数据
	})
	// 历史-日K线行情数据
	dCron.AddFunc("", "", func() {
		task.StockDailyMarketBatchUpdate(0)
	})
	// 历史-周K线行情数据(每周六更新一次)
	dCron.AddFunc("", "00 00 01 * * *", func() {
		task.StockDailyMarketBatchUpdate(1)
	})
	// 历史-月K线行情数据(每月1号更新一次)
	dCron.AddFunc("", "00 10 02 01 * *", func() {
		task.StockDailyMarketBatchUpdate(2)
	})
	// 每日资金流向排名(每小时更新一次)
	dCron.AddFunc("", "00 35 * * * *", func() {
		task.StockFundRankBatchUpdate()
	})
	// 每日龙虎榜(每小时更新一次)
	dCron.AddFunc("", "35 30 * * * *", func() {
		task.StockTigerLeaderBatchUpdate(svcCtx.SnowFlakeWorker)
	})
	// 每日个股异动(每5分钟更新一次)
	dCron.AddFunc("", "10 */5 * * * *", func() {
		task.OrderChangeBatchUpdate(svcCtx)
	})
	// 每日人气榜(每5分钟更新一次)
	dCron.AddFunc("", "20 */5 * * * *", func() {
		task.StockHotRankUpdate()
	})
	// 每日股评(每小时更新一次)
	dCron.AddFunc("", "30 30 * * * *", func() {
		task.StockDailyCommentBatchUpdate()
	})
	// 每日强势榜(每5分钟更新一次)
	dCron.AddFunc("", "30 */5 * * * *", func() {
		task.StockStrongPoolBatchUpdate()
	})
	// 每日行业-领涨股票(每小时更新一次)
	dCron.AddFunc("", "40 35 * * * *", func() {
		task.StockDailyIndustryUpdate()
	})
	// 每日概念-领涨股票(每小时更新一次)
	dCron.AddFunc("", "50 35 * * * *", func() {
		task.StockDailyConcept()
	})

	//
	//dCron.AddFunc("", "", func() {
	//
	//})

	go dCron.Start()
	handler.RegisterHandlers(server, svcCtx)

	logx.Infof("Starting API Server [%s:%d]", cfg.Host, cfg.Port)
	server.Start()
}
