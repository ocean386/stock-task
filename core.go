//	stocktask
//
//	Description: stocktask service
//
//	Schemes: http, https
//	Host: localhost:9100
//	BasePath: /
//	Version: 0.0.1
//	SecurityDefinitions:
//	  Token:
//	    type: apiKey
//	    name: Authorization
//	    in: header
//	Security:
//	  Token:
//	Consumes:
//	  - application/json
//
//	Produces:
//	  - application/json
//
// swagger:meta
package main

import (
	"flag"
	"fmt"
	"github.com/dcron-contrib/redisdriver"
	"github.com/libi/dcron"
	"github.com/ocean386/stock-task/internal/config"
	"github.com/ocean386/stock-task/internal/handler"
	"github.com/ocean386/stock-task/internal/logic/task"
	"github.com/ocean386/stock-task/internal/svc"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	"time"
)

var configFile = flag.String("f", "etc/core.yaml", "the config file")

func main() {
	flag.Parse()

	var cfg config.Config
	conf.MustLoad(*configFile, &cfg, conf.UseEnv())

	server := rest.MustNewServer(cfg.RestConf, rest.WithCors("*"))
	defer server.Stop()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisConf.Host,
		Password: cfg.RedisConf.Pass,
	})

	ctx := svc.NewServiceContext(cfg)

	redisDriver := redisdriver.NewDriver(redisClient)
	dCron := dcron.NewDcronWithOption("DCronServer", redisDriver,
		dcron.WithHashReplicas(10),
		dcron.WithNodeUpdateDuration(time.Second*10),
		dcron.CronOptionSeconds(),
	)

	dCron.AddFunc("StockHTTP", "*/5 * * * * *", task.StockTask)

	go dCron.Start()
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", cfg.Host, cfg.Port)
	server.Start()
}
