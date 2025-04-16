# tests/mock_modules.py
import sys
from unittest.mock import MagicMock

# Create mock modules
mock_modules = {
    'awsglue': MagicMock(),
    'awsglue.transforms': MagicMock(),
    'awsglue.utils': MagicMock(),
    'awsglue.context': MagicMock(),
    'awsglue.job': MagicMock()
}

# Add getResolvedOptions mock
mock_modules['awsglue.utils'].getResolvedOptions = MagicMock(return_value={
    'JOB_NAME': 'test-job',
    'S3_BUCKET_PATH': 'test-path',
    'ORDER_ITEMS_OUTPUT_PATH': 'test-order-items',
    'ORDERS_OUTPUT_PATH': 'test-orders',
    'PRODUCTS_OUTPUT_PATH': 'test-products',
    'REJECTED_PATH': 'test-rejected'
})

# Add to sys.modules
for name, module in mock_modules.items():
    sys.modules[name] = module