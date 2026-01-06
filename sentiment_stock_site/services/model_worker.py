import numpy as np
import pandas as pd
import joblib
import tensorflow as tf
from multiprocessing import Process, Queue
import warnings
warnings.filterwarnings('ignore')

class ModelWorker:
    def __init__(self):
        self.model = None
        self.X_scaler = None
        self.y_scaler = None
        self.initialized = False
        
    def initialize(self):
        """Инициализация модели в том же процессе"""
        if self.initialized:
            return
            
        print("Loading model and scalers...")
        # Загрузка модели Keras
        self.model = tf.keras.models.load_model(
            "models/generator_model.keras",
            compile=False
        )
        
        # Загрузка скейлеров
        self.X_scaler = joblib.load("models/X_scaler.pkl")
        self.y_scaler = joblib.load("models/y_scaler.pkl")
        
        # Оптимизация для CPU
        tf.config.threading.set_intra_op_parallelism_threads(1)
        tf.config.threading.set_inter_op_parallelism_threads(1)
        
        # Теплый запуск
        dummy_input = np.random.randn(1, 5, 15).astype(np.float32)
        _ = self.model.predict(dummy_input, verbose=0)
        
        print("Model initialized successfully")
        self.initialized = True
    
    def predict_batch(self, X_scaled):
        """Выполнение предсказания"""
        return self.model.predict(X_scaled, verbose=0, batch_size=1)

def worker_process(task_queue, result_queue):
    """Рабочий процесс для изоляции TensorFlow"""
    worker = ModelWorker()
    worker.initialize()
    
    print("Worker process started")
    
    while True:
        task = task_queue.get()
        if task is None:  # Сигнал завершения
            break
            
        try:
            if task['type'] == 'predict':
                X_scaled = task['data']
                result = worker.predict_batch(X_scaled)
                result_queue.put({'success': True, 'result': result})
            elif task['type'] == 'health':
                result_queue.put({'success': True, 'status': 'alive'})
        except Exception as e:
            result_queue.put({'success': False, 'error': str(e)})