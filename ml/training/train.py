#!/usr/bin/env python3
"""
ML Training Script with MLflow Tracking
Trains a simple model on order data stored in Delta Lake
"""

import os
import sys
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
import joblib
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")


def setup_mlflow():
    """Configure MLflow tracking"""
    # MLflow configuration for local setup
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("order_price_prediction")
    return mlflow


def load_data_from_delta():
    """Load and prepare data from Delta Lake"""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        # Create Spark session for Delta reading
        builder = SparkSession.builder \
            .appName("MLTraining") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Try to read from bronze Delta table
        bronze_path = "s3a://delta/bronze/orders"
        
        try:
            df = spark.read.format("delta").load(bronze_path)
            print(f"Loaded {df.count()} records from Delta Lake")
            
            # Convert to pandas for sklearn
            pandas_df = df.toPandas()
            spark.stop()
            return pandas_df
            
        except Exception as e:
            print(f"Could not read from Delta Lake: {e}")
            spark.stop()
            return None
            
    except ImportError:
        print("PySpark not available, generating synthetic data")
        return None


def generate_synthetic_data(n_samples=1000):
    """Generate synthetic order data for training"""
    np.random.seed(42)
    
    products = ['laptop', 'smartphone', 'tablet', 'headphones', 'keyboard']
    regions = ['us-east', 'us-west', 'eu-central', 'asia-pacific']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
    
    data = []
    for _ in range(n_samples):
        product = np.random.choice(products)
        region = np.random.choice(regions)
        currency = np.random.choice(currencies)
        quantity = np.random.randint(1, 6)
        
        # Simulate realistic pricing based on product and region
        base_prices = {
            'laptop': 800, 'smartphone': 500, 'tablet': 300,
            'headphones': 100, 'keyboard': 50
        }
        
        base_price = base_prices[product]
        
        # Add regional and currency variations
        regional_multiplier = {
            'us-east': 1.0, 'us-west': 1.1, 
            'eu-central': 1.2, 'asia-pacific': 0.9
        }
        
        price = base_price * regional_multiplier[region] * (0.8 + 0.4 * np.random.random())
        
        data.append({
            'product': product,
            'region': region,
            'currency': currency,
            'quantity': quantity,
            'price': round(price, 2)
        })
    
    return pd.DataFrame(data)


def prepare_features(df):
    """Feature engineering for price prediction"""
    # Handle Delta Lake data format
    if 'value' in df.columns:
        import json
        # Parse JSON from Delta Lake bronze table
        parsed_data = []
        for _, row in df.iterrows():
            try:
                order_data = json.loads(row['value'])
                parsed_data.append(order_data)
            except:
                continue
        df = pd.DataFrame(parsed_data)
    
    # Basic feature engineering
    features = df.copy()
    
    # Encode categorical variables
    label_encoders = {}
    categorical_cols = ['product', 'region', 'currency']
    
    for col in categorical_cols:
        if col in features.columns:
            le = LabelEncoder()
            features[f'{col}_encoded'] = le.fit_transform(features[col])
            label_encoders[col] = le
    
    # Create additional features
    if 'quantity' in features.columns:
        features['quantity_squared'] = features['quantity'] ** 2
        features['quantity_log'] = np.log1p(features['quantity'])
    
    # Product-region interaction
    if 'product_encoded' in features.columns and 'region_encoded' in features.columns:
        features['product_region_interaction'] = features['product_encoded'] * features['region_encoded']
    
    return features, label_encoders


def train_model(X, y):
    """Train Random Forest model"""
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    # Metrics
    metrics = {
        'train_rmse': np.sqrt(mean_squared_error(y_train, y_pred_train)),
        'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
        'train_mae': mean_absolute_error(y_train, y_pred_train),
        'test_mae': mean_absolute_error(y_test, y_pred_test),
        'train_r2': r2_score(y_train, y_pred_train),
        'test_r2': r2_score(y_test, y_pred_test)
    }
    
    return model, metrics, X_test, y_test, y_pred_test


def main():
    """Main training pipeline"""
    print("Starting ML Training Pipeline...")
    
    # Setup MLflow
    mlflow_client = setup_mlflow()
    
    with mlflow.start_run():
        # Log parameters
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("data_source", "delta_lake_or_synthetic")
        
        # Load data
        print("Loading data...")
        df = load_data_from_delta()
        
        if df is None or len(df) < 100:
            print("Using synthetic data for training")
            df = generate_synthetic_data(1000)
            mlflow.log_param("data_type", "synthetic")
        else:
            mlflow.log_param("data_type", "real_delta")
        
        print(f"Dataset shape: {df.shape}")
        mlflow.log_param("dataset_size", len(df))
        
        # Feature engineering
        print("Preparing features...")
        features, label_encoders = prepare_features(df)
        
        # Select feature columns for training
        feature_cols = [col for col in features.columns 
                       if col.endswith('_encoded') or col in ['quantity', 'quantity_squared', 'quantity_log', 'product_region_interaction']]
        
        if not feature_cols:
            print("Error: No suitable features found")
            return
        
        X = features[feature_cols]
        y = features['price']
        
        print(f"Feature columns: {feature_cols}")
        mlflow.log_param("features", feature_cols)
        
        # Train model
        print("Training model...")
        model, metrics, X_test, y_test, y_pred_test = train_model(X, y)
        
        # Log metrics
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
            print(f"{metric_name}: {metric_value:.4f}")
        
        # Log model
        mlflow.sklearn.log_model(
            model, 
            "random_forest_price_predictor",
            registered_model_name="OrderPricePredictor"
        )
        
        # Save label encoders and feature info
        model_artifacts = {
            'model': model,
            'label_encoders': label_encoders,
            'feature_columns': feature_cols
        }
        
        artifact_path = f"/tmp/model_artifacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
        joblib.dump(model_artifacts, artifact_path)
        mlflow.log_artifact(artifact_path, "model_artifacts")
        
        # Feature importance
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print("\nFeature Importance:")
            print(feature_importance)
            
            # Save feature importance plot
            import matplotlib.pyplot as plt
            plt.figure(figsize=(10, 6))
            plt.barh(feature_importance['feature'], feature_importance['importance'])
            plt.title('Feature Importance')
            plt.tight_layout()
            
            importance_plot_path = f"/tmp/feature_importance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(importance_plot_path)
            mlflow.log_artifact(importance_plot_path, "plots")
            plt.close()
        
        # Log sample predictions
        sample_predictions = pd.DataFrame({
            'actual': y_test.head(10),
            'predicted': y_pred_test[:10]
        })
        
        print("\nSample Predictions:")
        print(sample_predictions)
        
        print(f"\nModel training completed! Run ID: {mlflow.active_run().info.run_id}")
        print("Check MLflow UI at http://localhost:5000 to view results")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error in training pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)