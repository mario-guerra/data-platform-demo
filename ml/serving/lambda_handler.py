#!/usr/bin/env python3
"""
Local Lambda-style ML serving handler
Loads trained model and provides prediction endpoint
"""

import json
import os
import sys
from typing import Dict, Any, Optional
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

# Optional imports with fallbacks
try:
    import joblib
except ImportError:
    joblib = None

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import mlflow
    import mlflow.sklearn
except ImportError:
    mlflow = None


class ModelPredictor:
    """Model prediction handler"""
    
    def __init__(self):
        self.model = None
        self.label_encoders = None
        self.feature_columns = None
        self.fallback_mean_price = 250.0  # Fallback prediction
        self.load_model()
    
    def load_model(self):
        """Load the trained model from MLflow or fallback"""
        if mlflow is None:
            print("MLflow not available, using fallback model")
            self.model = None
            return
            
        try:
            # Try to load from MLflow
            mlflow.set_tracking_uri("http://localhost:5000")
            
            # Get the latest version of the registered model
            client = mlflow.tracking.MlflowClient()
            
            try:
                model_versions = client.search_model_versions("name='OrderPricePredictor'")
                if model_versions:
                    latest_version = max(model_versions, key=lambda x: int(x.version))
                    model_uri = f"models:/OrderPricePredictor/{latest_version.version}"
                    
                    print(f"Loading model version {latest_version.version} from MLflow...")
                    self.model = mlflow.sklearn.load_model(model_uri)
                    
                    # Try to load additional artifacts (label encoders, etc.)
                    # This is a simplified approach - in production, you'd save these as artifacts
                    print("Model loaded successfully from MLflow")
                    return
                    
            except Exception as e:
                print(f"Could not load from MLflow registry: {e}")
            
            # Fallback: try to load latest run's model
            experiment = mlflow.get_experiment_by_name("order_price_prediction")
            if experiment:
                runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
                if not runs.empty:
                    latest_run = runs.iloc[0]
                    model_uri = f"runs:/{latest_run.run_id}/random_forest_price_predictor"
                    
                    print(f"Loading model from latest run: {latest_run.run_id}")
                    self.model = mlflow.sklearn.load_model(model_uri)
                    print("Model loaded successfully from latest run")
                    return
                    
        except Exception as e:
            print(f"Error loading model from MLflow: {e}")
        
        print("Using fallback mean prediction model")
        self.model = None
    
    def predict(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Make prediction for a single order"""
        try:
            if self.model is None:
                return {
                    "predicted_price": self.fallback_mean_price,
                    "confidence": 0.5,
                    "model_type": "fallback_mean",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Prepare features (simplified version)
            features = self.prepare_features(order_data)
            
            if features is None:
                return {
                    "predicted_price": self.fallback_mean_price,
                    "confidence": 0.3,
                    "model_type": "fallback_mean",
                    "error": "Could not prepare features",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Make prediction
            prediction = self.model.predict([features])[0]
            
            # Calculate confidence (simplified)
            confidence = min(0.9, max(0.1, 1.0 - abs(prediction - self.fallback_mean_price) / self.fallback_mean_price))
            
            return {
                "predicted_price": round(float(prediction), 2),
                "confidence": round(confidence, 3),
                "model_type": "random_forest",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Error in prediction: {e}")
            return {
                "predicted_price": self.fallback_mean_price,
                "confidence": 0.1,
                "model_type": "fallback_mean",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def prepare_features(self, order_data: Dict[str, Any]) -> Optional[list]:
        """Prepare features for prediction"""
        try:
            # Simple feature encoding (in production, use saved label encoders)
            product_encoding = {
                'laptop': 0, 'smartphone': 1, 'tablet': 2, 
                'headphones': 3, 'keyboard': 4
            }
            
            region_encoding = {
                'us-east': 0, 'us-west': 1, 
                'eu-central': 2, 'asia-pacific': 3
            }
            
            currency_encoding = {
                'USD': 0, 'EUR': 1, 'GBP': 2, 'JPY': 3, 'CAD': 4
            }
            
            # Extract and encode features
            product = order_data.get('product', 'laptop')
            region = order_data.get('region', 'us-east')
            currency = order_data.get('currency', 'USD')
            quantity = order_data.get('quantity', 1)
            
            product_encoded = product_encoding.get(product, 0)
            region_encoded = region_encoding.get(region, 0)
            currency_encoded = currency_encoding.get(currency, 0)
            
            # Create feature vector (must match training features)
            features = [
                product_encoded,
                region_encoded,
                currency_encoded,
                quantity,
                quantity ** 2,
                np.log1p(quantity) if np else 0,  # Fallback if numpy not available
                product_encoded * region_encoded
            ]
            
            return features
            
        except Exception as e:
            print(f"Error preparing features: {e}")
            return None


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """AWS Lambda-style handler function"""
    try:
        # Parse the event
        if isinstance(event, str):
            event = json.loads(event)
        
        # Get order data from event
        order_data = event.get('body', event)
        if isinstance(order_data, str):
            order_data = json.loads(order_data)
        
        # Initialize predictor (in production, this would be cached)
        predictor = ModelPredictor()
        
        # Make prediction
        result = predictor.predict(order_data)
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(result)
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
        }


def test_local_server():
    """Run a simple local test server"""
    print("Starting local ML serving test...")
    
    # Test data
    test_orders = [
        {
            "product": "laptop",
            "region": "us-east",
            "currency": "USD",
            "quantity": 1
        },
        {
            "product": "smartphone",
            "region": "eu-central",
            "currency": "EUR",
            "quantity": 2
        },
        {
            "product": "headphones",
            "region": "asia-pacific",
            "currency": "JPY",
            "quantity": 3
        }
    ]
    
    predictor = ModelPredictor()
    
    print("\nTesting predictions:")
    print("-" * 50)
    
    for i, order in enumerate(test_orders, 1):
        print(f"\nTest {i}: {order}")
        result = predictor.predict(order)
        print(f"Prediction: {json.dumps(result, indent=2)}")
    
    print("\n" + "-" * 50)
    print("Local serving test completed!")
    
    # Test Lambda handler format
    print("\nTesting Lambda handler format:")
    event = {
        "body": json.dumps(test_orders[0])
    }
    
    response = lambda_handler(event)
    print(f"Lambda response: {json.dumps(response, indent=2)}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_local_server()
    else:
        # Interactive mode
        print("ML Serving Handler - Interactive Mode")
        print("Enter order data as JSON, or 'quit' to exit")
        
        predictor = ModelPredictor()
        
        while True:
            try:
                user_input = input("\nOrder JSON: ").strip()
                
                if user_input.lower() in ['quit', 'exit', 'q']:
                    break
                
                if not user_input:
                    # Use example order
                    order_data = {
                        "product": "laptop",
                        "region": "us-east", 
                        "currency": "USD",
                        "quantity": 1
                    }
                    print(f"Using example: {order_data}")
                else:
                    order_data = json.loads(user_input)
                
                result = predictor.predict(order_data)
                print(f"Prediction: {json.dumps(result, indent=2)}")
                
            except KeyboardInterrupt:
                break
            except json.JSONDecodeError:
                print("Invalid JSON format. Please try again.")
            except Exception as e:
                print(f"Error: {e}")
        
        print("\nGoodbye!")