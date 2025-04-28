from faker import Faker
from datetime import datetime, timedelta
import uuid
import random
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import json

class TransactionFakerModel:
    """
    Base class for generating fake transaction data with 100 columns.
    This can be used for testing or development of online transaction systems.
    """
    
    def __init__(self, locale: str = 'en_US', seed: Optional[int] = None):
        """
        Initialize the transaction faker model.
        
        Args:
            locale: The locale to use for the faker instance
            seed: Optional seed for reproducible data generation
        """
        self.faker = Faker(locale)
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
            np.random.seed(seed)
        
        # Define payment status options
        self.payment_statuses = ['completed', 'pending', 'failed', 'refunded', 'cancelled', 'disputed']
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer', 'crypto']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CNY', 'INR']
        self.product_categories = ['electronics', 'clothing', 'books', 'home', 'beauty', 'sports', 'toys', 'food', 'digital']
        self.shipping_methods = ['standard', 'express', 'overnight', 'two_day', 'international', 'local', 'pickup']
        self.tax_types = ['sales_tax', 'VAT', 'GST', 'no_tax', 'reduced_rate']
        self.device_types = ['desktop', 'mobile', 'tablet', 'app', 'other']
        self.browser_types = ['chrome', 'firefox', 'safari', 'edge', 'opera', 'ie', 'other']
        self.promo_types = ['percentage', 'fixed_amount', 'free_shipping', 'bogo', 'none']
        self.fraud_indicators = ['none', 'suspicious_ip', 'multiple_attempts', 'address_mismatch', 'high_value']
    
    def generate_transaction_data(self) -> Dict[str, Any]:
        """
        Generate a single transaction record with 100 columns.
        
        Returns:
            Dictionary with transaction data
        """
        # Generate a transaction date between 1 year ago and now
        transaction_date = self.faker.date_time_between(start_date='-1y', end_date='now')
        
        # Calculate related dates
        shipping_date = transaction_date + timedelta(days=random.randint(0, 3))
        delivery_date = shipping_date + timedelta(days=random.randint(1, 7))
        
        # Base amount and calculations
        base_amount = round(random.uniform(10.0, 1000.0), 2)
        tax_rate = round(random.uniform(0.0, 0.25), 2)
        tax_amount = round(base_amount * tax_rate, 2)
        shipping_cost = round(random.uniform(0.0, 50.0), 2)
        discount_amount = round(random.uniform(0.0, base_amount * 0.3), 2) if random.random() < 0.3 else 0.0
        total_amount = round(base_amount + tax_amount + shipping_cost - discount_amount, 2)
        
        # Determine payment status
        payment_status = random.choice(self.payment_statuses)
        
        # Create transaction dictionary
        transaction = {
            # 1-10: Basic transaction information
            'transaction_id': str(uuid.uuid4()),
            'order_id': self.faker.random_number(digits=8),
            'transaction_date': transaction_date.strftime("%Y-%m-%d"),
            'transaction_time': transaction_date.strftime('%H:%M:%S'),
            'transaction_timestamp': int(transaction_date.timestamp()),
            'payment_status': payment_status,
            'payment_method': random.choice(self.payment_methods),
            'currency': random.choice(self.currencies),
            'total_amount': total_amount,
            'transaction_type': 'purchase' if random.random() < 0.8 else 'refund',
            
            # 11-20: Customer information
            'customer_id': random.randint(999,4999),
            'customer_email': self.faker.email(),
            'customer_name': self.faker.name(),
            'customer_phone': self.faker.phone_number(),
            'customer_ip_address': self.faker.ipv4(),
            'customer_country': self.faker.country_code(),
            'customer_city': self.faker.city(),
            'customer_postal_code': self.faker.postcode(),
            'customer_account_age_days': random.randint(1, 3650),
            'customer_loyalty_level': random.choice(['bronze', 'silver', 'gold', 'platinum', 'new']),
            
            # 21-30: Product information
            'product_id': self.faker.random_number(digits=6),
            'product_name': self.faker.bs(),
            'product_category': random.choice(self.product_categories),
            'product_quantity': random.randint(1, 10),
            'product_unit_price': round(base_amount / random.randint(1, 5), 2),
            'product_sku': f"SKU-{self.faker.random_number(digits=6)}",
            'product_weight_kg': round(random.uniform(0.1, 25.0), 2),
            'product_is_digital': random.choice([True, False]),
            'product_is_subscription': random.choice([True, False]),
            'product_rating': round(random.uniform(1.0, 5.0), 1),
            
            # 31-40: Payment details
            'payment_processor': random.choice(['stripe', 'paypal', 'square', 'adyen', 'braintree']),
            'payment_processor_fee': round(total_amount * random.uniform(0.01, 0.05), 2),
            'payment_card_type': random.choice(['visa', 'mastercard', 'amex', 'discover', 'other']),
            'payment_card_last4': ''.join(random.choices('0123456789', k=4)),
            'payment_confirmed': payment_status in ['completed', 'refunded'],
            'payment_attempts': random.randint(1, 3),
            'payment_authorization_code': self.faker.uuid4()[:8].upper(),
            'payment_verified_address': random.choice([True, False]),
            'payment_verified_zipcode': random.choice([True, False]),
            'payment_verified_cvv': random.choice([True, False]),
            
            # 41-50: Shipping information
            'shipping_address': self.faker.street_address(),
            'shipping_city': self.faker.city(),
            'shipping_state': self.faker.state_abbr(),
            'shipping_country': self.faker.country_code(),
            'shipping_postal_code': self.faker.postcode(),
            'shipping_method': random.choice(self.shipping_methods),
            'shipping_carrier': random.choice(['usps', 'fedex', 'ups', 'dhl', 'amazon']),
            'shipping_tracking_number': f"TRK{self.faker.random_number(digits=12)}",
            'shipping_date': shipping_date.strftime("%Y-%m-%d"),
            'estimated_delivery_date': delivery_date.strftime("%Y-%m-%d"),
            
            # 51-60: Tax and financial details
            'subtotal_amount': base_amount,
            'tax_amount': tax_amount,
            'tax_rate': tax_rate,
            'tax_type': random.choice(self.tax_types),
            'tax_jurisdiction': self.faker.state(),
            'shipping_cost': shipping_cost,
            'handling_fee': round(random.uniform(0, 10.0), 2),
            'insurance_fee': round(random.uniform(0, 15.0), 2),
            'discount_amount': discount_amount,
            'discount_code': f"PROMO{self.faker.random_number(digits=5)}" if discount_amount > 0 else None,
            
            # 61-70: Merchant information
            'merchant_id': f"MERCH-{self.faker.random_number(digits=6)}",
            'merchant_name': self.faker.company(),
            'merchant_category_code': f"{random.randint(1000, 9999)}",
            'store_id': f"STORE-{self.faker.random_number(digits=4)}",
            'store_name': f"{self.faker.company()} - {self.faker.city()}",
            'terminal_id': f"TERM-{self.faker.random_number(digits=4)}",
            'merchant_country': self.faker.country_code(),
            'merchant_currency': random.choice(self.currencies),
            'merchant_settlement_amount': round(total_amount * 0.97, 2),
            'merchant_settlement_date': (transaction_date + timedelta(days=random.randint(1, 3))).strftime("%Y-%m-%d %H:%M:%S"),
            
            # 71-80: Transaction metadata
            'device_type': random.choice(self.device_types),
            'browser_type': random.choice(self.browser_types),
            'user_agent': self.faker.user_agent(),
            'referral_source': random.choice(['direct', 'search', 'social', 'email', 'affiliate']),
            'utm_source': random.choice(['google', 'facebook', 'instagram', 'email', None]),
            'utm_medium': random.choice(['cpc', 'organic', 'social', 'email', None]),
            'utm_campaign': f"campaign-{self.faker.random_number(digits=4)}" if random.random() < 0.7 else None,
            'session_id': f"SESSION-{self.faker.random_number(digits=10)}",
            'is_mobile': random.choice([True, False]),
            'is_app': random.choice([True, False]),
            
            # 81-90: Risk and fraud parameters
            'risk_score': random.randint(0, 100),
            'fraud_score': random.randint(0, 100),
            'fraud_indicators': random.choice(self.fraud_indicators),
            'address_verification_result': random.choice(['match', 'partial', 'no_match', 'not_checked']),
            'cvv_verification_result': random.choice(['match', 'no_match', 'not_checked']),
            'geo_ip_distance_km': round(random.uniform(0, 15000), 2),
            'is_high_risk_country': random.choice([True, False]),
            'is_proxy_ip': random.choice([True, False]),
            'is_vpn': random.choice([True, False]),
            'chargeback_probability': round(random.uniform(0, 1), 3),
            
            # 91-100: Miscellaneous
            'promo_type': random.choice(self.promo_types),
            'gift_message': self.faker.text(max_nb_chars=50) if random.random() < 0.2 else None,
            'is_gift': random.choice([True, False]),
            'has_digital_receipt': random.choice([True, False]),
            'customer_notes': self.faker.text(max_nb_chars=100) if random.random() < 0.1 else None,
            'internal_notes': self.faker.text(max_nb_chars=100) if random.random() < 0.1 else None,
            'created_by': self.faker.user_name() if random.random() < 0.5 else 'system',
            'updated_at': (transaction_date + timedelta(minutes=random.randint(10, 120))).strftime("%Y-%m-%d %H:%M:%S"),
            'is_test_transaction': random.random() < 0.05,
            'environment': random.choice(['production', 'staging', 'development', 'test'])
        }
        
        return transaction
    
    def generate_transactions(self, num_transactions: int = 100) -> List[Dict[str, Any]]:
        """
        Generate multiple transaction records.
        
        Args:
            num_transactions: Number of transactions to generate
            
        Returns:
            List of transaction dictionaries
        """
        return [self.generate_transaction_data() for _ in range(num_transactions)]
    
    


# Example usage
if __name__ == "__main__":
    # Create transaction faker model
    transaction_faker = TransactionFakerModel(seed=42)
    
    # Generate 5 transactions
    transactions = transaction_faker.generate_transactions(3)
    
    # Print first transaction    
    print(json.dumps(transactions, default=str, indent=2))
    
    