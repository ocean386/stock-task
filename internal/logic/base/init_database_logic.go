package base

import (
	"context"
	"fmt"
	"github.com/ocean386/stock-task/internal/logic/task"
	"time"

	"github.com/ocean386/stock-task/internal/svc"
	"github.com/ocean386/stock-task/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type InitDatabaseLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewInitDatabaseLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InitDatabaseLogic {
	return &InitDatabaseLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx}
}

func (l *InitDatabaseLogic) InitDatabase() (resp *types.BaseMsgResp, err error) {

	logx.Infof("执行 InitDatabase 任务:%v", time.Now().Format("15:04:05"))

	resp = &types.BaseMsgResp{}

	exists, err := l.svcCtx.Redis.ExistsCtx(l.ctx, "StockInit")
	if err != nil {
		logx.Errorf("[初始化数据] Failed to check Redis key existence: %v", err)
		resp.Code = 500
		resp.Msg = "redis error"
		return resp, err
	}

	if !exists {
		err = l.svcCtx.Redis.SetexCtx(l.ctx, "StockInit", "1", 3600)
		if err != nil {
			logx.Errorf("[初始化数据] Failed to set Redis key: %v", err)
			resp.Code = 500
			resp.Msg = "redis error"
			return resp, err
		}
		go func() {
			task.StockRealTimeMarketDataBatchUpdate(l.svcCtx.Redis, 1) // 0-流通市值 1-实时行情数据
			task.StockFundRankBatchUpdate()                            // 每日资金流向排名
			task.StockTigerLeaderBatchUpdate(l.svcCtx.SnowFlakeWorker) // 每日龙虎榜
			task.OrderChangeBatchUpdate(l.svcCtx.Redis)                // 每日个股异动
			task.StockHotRankUpdate(l.svcCtx.Redis)                    // 每日人气榜
			task.StockDailyCommentBatchUpdate(l.svcCtx.Redis)          // 每日股评
			task.StockStrongPoolBatchUpdate(l.svcCtx.Redis)            // 每日强势榜
			task.StockDailyIndustryUpdate()                            // 每日行业-领涨股票
			task.StockDailyConcept()                                   // 每日概念-领涨股票
		}()
	}

	resp.Code = 200
	resp.Msg = fmt.Sprintf("Success %v", time.Now().Format("15:04:05"))
	return
}
