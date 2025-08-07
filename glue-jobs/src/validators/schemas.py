from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

class CustomerSchema(BaseModel):
    """Schema for customer data validation"""
    customer_id: str = Field(..., min_length=1, max_length=50)
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr
    phone: Optional[str] = Field(None, regex=r'^\(\d{3}\) \d{3}-\d{4}$')
    birth_date: Optional[datetime] = None
    registration_date: datetime
    status: str = Field(..., regex=r'^(active|inactive|suspended)$')

class SalesSchema(BaseModel):
    """Schema for sales data validation"""
    transaction_id: str = Field(..., min_length=1, max_length=50)
    customer_id: str = Field(..., min_length=1, max_length=50)
    product_id: str = Field(..., min_length=1, max_length=50)
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., gt=0)
    total_amount: Decimal = Field(..., gt=0)
    transaction_date: datetime
    sales_rep_id: Optional[str] = None
    discount_applied: Optional[Decimal] = Field(None, ge=0)

class ProductSchema(BaseModel):
    """Schema for product catalog validation"""
    product_id: str = Field(..., min_length=1, max_length=50)
    product_name: str = Field(..., min_length=1, max_length=200)
    category: str = Field(..., min_length=1, max_length=100)
    price: Decimal = Field(..., gt=0)
    cost: Optional[Decimal] = Field(None, gt=0)
    supplier_id: Optional[str] = None
    is_active: bool = True
    created_date: datetime
    last_updated: datetime

class InventorySchema(BaseModel):
    """Schema for inventory data validation"""
    product_id: str = Field(..., min_length=1, max_length=50)
    location_id: str = Field(..., min_length=1, max_length=50)
    quantity_on_hand: int = Field(..., ge=0)
    quantity_reserved: int = Field(..., ge=0)
    reorder_point: int = Field(..., ge=0)
    max_stock_level: int = Field(..., gt=0)
    last_count_date: Optional[datetime] = None
    cost_per_unit: Optional[Decimal] = Field(None, gt=0)