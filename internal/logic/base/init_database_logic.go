package base

import (
	"context"
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

	go task.StockRealTimeMarketDataBatchUpdate()

	exists, err := l.svcCtx.Redis.ExistsCtx(l.ctx, "StockInit")
	if err != nil {
		logx.Errorf("Failed to check Redis key existence: %v", err)
		resp.Code = 500
		resp.Msg = "Failed to check Redis key existence"
		return resp, err
	}

	if !exists {
		err = l.svcCtx.Redis.SetexCtx(l.ctx, "StockInit", "1", 3600)
		if err != nil {
			logx.Errorf("Failed to set Redis key: %v", err)
			resp.Code = 500
			resp.Msg = "Failed to set Redis key"
			return resp, err
		}
		//go StockTask()
	}

	resp.Code = 200
	resp.Msg = "Success"
	return
}
