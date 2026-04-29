from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import pickle, numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

# -----

@app.post("/score")
async def score_transaction(tx: Transaction):
    data = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    
    prediction = model.predict(data)[0]
    
    probability = model.predict_proba(data)[0][1]
    
    return {
        "is_fraud": bool(prediction),
        "fraud_probability": float(probability)
    }

@app.get("/health")
async def health_check():
    """
    Endpoint do monitorowania stanu API.
    Zwraca status 200 OK, jeśli aplikacja działa.
    """
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "timestamp": datetime.now().isoformat()
    }