# -*- coding: utf-8 -*-
"""
===================================
通用响应模型
===================================

职责：
1. 定义通用的响应模型（HealthResponse, ErrorResponse 等）
2. 提供统一的响应格式
"""

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class RootResponse(BaseModel):
    """API 根路由响应"""
    
    message: str = Field(..., description="API 运行状态消息", json_schema_extra={"example": "Daily Stock Analysis API is running"})
    version: Optional[str] = Field(None, description="API 版本", json_schema_extra={"example": "1.0.0"})
    
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "message": "Daily Stock Analysis API is running",
            "version": "1.0.0"
        }
    })


class HealthResponse(BaseModel):
    """健康检查响应"""
    
    status: str = Field(..., description="服务状态", json_schema_extra={"example": "ok"})
    timestamp: Optional[str] = Field(None, description="时间戳")
    
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "status": "ok",
            "timestamp": "2024-01-01T12:00:00"
        }
    })


class ReadinessCheckResponse(BaseModel):
    """单项就绪检查结果。"""

    status: Literal["ready", "not_ready", "skipped"] = Field(..., description="检查状态")
    detail: str = Field(..., description="稳定、非敏感的结果代码")
    required: bool = Field(True, description="该检查是否影响整体就绪状态")


class ReadinessResponse(BaseModel):
    """服务就绪检查响应。"""

    status: Literal["ready", "not_ready"] = Field(..., description="服务是否可接收流量")
    timestamp: str = Field(..., description="检查时间戳")
    checks: Dict[str, ReadinessCheckResponse] = Field(..., description="各依赖检查结果")


class ErrorResponse(BaseModel):
    """错误响应"""
    
    error: str = Field(..., description="错误类型", json_schema_extra={"example": "validation_error"})
    message: str = Field(..., description="错误详情", json_schema_extra={"example": "请求参数错误"})
    detail: Optional[Any] = Field(None, description="附加错误信息")
    
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "error": "not_found",
            "message": "资源不存在",
            "detail": None
        }
    })


class SuccessResponse(BaseModel):
    """通用成功响应"""
    
    success: bool = Field(True, description="是否成功")
    message: Optional[str] = Field(None, description="成功消息")
    data: Optional[Any] = Field(None, description="响应数据")
    
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "message": "操作成功",
            "data": None
        }
    })
