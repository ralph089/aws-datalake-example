import pandas as pd
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import structlog

logger = structlog.get_logger()

class DataQualityChecker:
    """Free data quality checks without commercial tools"""
    
    @staticmethod
    def check_completeness(df: DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Check if required columns are present and populated"""
        results = {
            "passed": True,
            "checks": {}
        }
        
        record_count = df.count()
        
        for col_name in required_columns:
            if col_name not in df.columns:
                results["checks"][col_name] = {"exists": False}
                results["passed"] = False
            else:
                null_count = df.filter(col(col_name).isNull()).count()
                null_percentage = (null_count / record_count) * 100 if record_count > 0 else 0
                results["checks"][col_name] = {
                    "exists": True,
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "passed": null_percentage < 5  # Less than 5% nulls
                }
                if null_percentage >= 5:
                    results["passed"] = False
        
        return results
    
    @staticmethod
    def check_uniqueness(df: DataFrame, unique_columns: List[str]) -> Dict[str, Any]:
        """Check for duplicate values in columns that should be unique"""
        results = {
            "passed": True,
            "checks": {}
        }
        
        for col_name in unique_columns:
            if col_name in df.columns:
                total_count = df.count()
                unique_count = df.select(col_name).distinct().count()
                duplicate_count = total_count - unique_count
                
                results["checks"][col_name] = {
                    "total_count": total_count,
                    "unique_count": unique_count,
                    "duplicate_count": duplicate_count,
                    "passed": duplicate_count == 0
                }
                if duplicate_count > 0:
                    results["passed"] = False
        
        return results
    
    @staticmethod
    def check_ranges(df: DataFrame, range_checks: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Check if numeric values are within expected ranges"""
        results = {
            "passed": True,
            "checks": {}
        }
        
        for col_name, constraints in range_checks.items():
            if col_name in df.columns:
                min_val = constraints.get("min")
                max_val = constraints.get("max")
                
                out_of_range_count = 0
                if min_val is not None:
                    out_of_range_count += df.filter(col(col_name) < min_val).count()
                if max_val is not None:
                    out_of_range_count += df.filter(col(col_name) > max_val).count()
                
                results["checks"][col_name] = {
                    "out_of_range_count": out_of_range_count,
                    "min_constraint": min_val,
                    "max_constraint": max_val,
                    "passed": out_of_range_count == 0
                }
                if out_of_range_count > 0:
                    results["passed"] = False
        
        return results
    
    @staticmethod
    def check_patterns(df: DataFrame, pattern_checks: Dict[str, str]) -> Dict[str, Any]:
        """Check if string values match expected patterns (regex)"""
        results = {
            "passed": True,
            "checks": {}
        }
        
        for col_name, pattern in pattern_checks.items():
            if col_name in df.columns:
                total_count = df.filter(col(col_name).isNotNull()).count()
                valid_count = df.filter(col(col_name).rlike(pattern)).count()
                invalid_count = total_count - valid_count
                
                results["checks"][col_name] = {
                    "total_count": total_count,
                    "valid_count": valid_count,
                    "invalid_pattern_count": invalid_count,
                    "pattern": pattern,
                    "passed": invalid_count == 0
                }
                if invalid_count > 0:
                    results["passed"] = False
        
        return results
    
    @staticmethod
    def check_referential_integrity(df: DataFrame, reference_checks: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Check referential integrity between columns or external data"""
        results = {
            "passed": True,
            "checks": {}
        }
        
        for check_name, check_config in reference_checks.items():
            col_name = check_config["column"]
            valid_values = check_config["valid_values"]
            
            if col_name in df.columns:
                total_count = df.filter(col(col_name).isNotNull()).count()
                valid_count = df.filter(col(col_name).isin(valid_values)).count()
                invalid_count = total_count - valid_count
                
                results["checks"][check_name] = {
                    "column": col_name,
                    "total_count": total_count,
                    "valid_count": valid_count,
                    "invalid_count": invalid_count,
                    "passed": invalid_count == 0
                }
                if invalid_count > 0:
                    results["passed"] = False
        
        return results