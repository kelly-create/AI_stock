# -*- coding: utf-8 -*-
"""
===================================
健康检查接口
===================================

职责：
1. 提供 /api/v1/health 健康检查接口
2. 提供 /api/v1/health/ready 真实就绪检查接口
3. 用于负载均衡器和监控系统
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.v1.schemas.common import (
    HealthResponse,
    ReadinessCheckResponse,
    ReadinessResponse,
)
from src.services.readiness_service import ReadinessService

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    健康检查接口
    
    用于负载均衡器或监控系统检查服务状态
    
    Returns:
        HealthResponse: 包含服务状态和时间戳
    """
    return HealthResponse(
        status="ok",
        timestamp=datetime.now().isoformat()
    )


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={503: {"model": ReadinessResponse, "description": "服务尚未就绪"}},
    response_description="服务可接收流量",
    operation_id="readinessCheck",
    summary="服务就绪检查",
    description=(
        "检查数据库迁移状态、SQLite 读写能力，以及启用 Durable Worker 后的可选心跳。"
        "未就绪时返回 HTTP 503；该接口不会执行数据库迁移。"
    ),
)
def readiness_check(request: Request) -> ReadinessResponse | JSONResponse:
    """Return traffic readiness without changing the liveness contract."""

    service = getattr(request.app.state, "readiness_service", None)
    if service is None:
        service = ReadinessService(
            worker_heartbeat_checker=getattr(
                request.app.state,
                "readiness_worker_heartbeat_checker",
                None,
            ),
            require_worker_heartbeat=bool(
                getattr(request.app.state, "readiness_require_worker_heartbeat", False)
            ),
        )
    report = service.check()
    payload = ReadinessResponse(
        status="ready" if report.ready else "not_ready",
        timestamp=datetime.now(timezone.utc).isoformat(),
        checks={
            name: ReadinessCheckResponse(
                status=check.status,
                detail=check.detail,
                required=check.required,
            )
            for name, check in report.checks.items()
        },
    )
    if report.ready:
        return payload
    return JSONResponse(status_code=503, content=payload.model_dump())
