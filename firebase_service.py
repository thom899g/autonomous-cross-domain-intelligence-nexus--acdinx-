"""
Firebase Service Module for ACDINX
Handles all Firebase interactions with robust error handling and connection management
"""

import json
import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from contextlib import contextmanager

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.exceptions import FirebaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FirebaseService:
    """Manages Firebase connections and operations with robust error handling"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.db = None
            self.initialized = False
            self._service_account_path = None
            FirebaseService._initialized = True
    
    def initialize(self, service_account_path: str) -> bool:
        """
        Initialize Firebase connection with error handling and validation
        
        Args:
            service_account_path: Path to Firebase service account JSON file
            
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            # Validate file exists and is readable
            if not os.path.exists(service_account_path):
                logger.error(f"Service account file not found: {service_account_path}")
                return False
            
            if not os.access(service_account_path, os.R_OK):
                logger.error(f"Service account file not readable: {service_account_path}")
                return False
            
            # Load and validate service account JSON
            with open(service_account_path, 'r') as f:
                service_account = json.load(f)
            
            required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 
                             'client_email', 'client_id', 'auth_uri', 'token_uri']
            for field in required_fields:
                if field not in service_account:
                    logger.error(f"Missing required field in service account: {field}")
                    return False
            
            # Initialize Firebase app
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            self._service_account_path = service_account_path
            self.initialized = True
            
            logger.info(f"Firebase initialized successfully for project: {service_account['project_id']}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in service account file: {e}")
            return False
        except FirebaseError as e:
            logger.error(f"Firebase initialization error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during Firebase initialization: {e}")
            return False
    
    @contextmanager
    def get_collection(self, collection_name: str):
        """
        Context manager for safe collection access with automatic error handling
        
        Args:
            collection_name: Name of Firestore collection
            
        Yields:
            Firestore collection reference
            
        Raises:
            RuntimeError: If Firebase not initialized
            ValueError: If collection_name is invalid
        """
        if not self.initialized or self.db is None:
            raise RuntimeError("Firebase not initialized. Call initialize() first.")
        
        if not collection_name or not isinstance(collection_name, str):
            raise ValueError("collection_name must be a non-empty string")
        
        try:
            collection_ref = self.db.collection(collection_name)
            yield collection_ref
        except FirebaseError as e:
            logger.error(f"Firebase error accessing collection {collection_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error accessing collection {collection_name}: {e}")
            raise
    
    def set_document(self, collection_name: str, document_id: str, data: Dict[str, Any]) -> bool:
        """
        Set document in Firestore with comprehensive error handling
        
        Args:
            collection_name: Firestore collection name
            document_id: Document ID
            data: Document data
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.initialized:
            logger.error("Firebase not initialized")
            return False
        
        if not data or not isinstance(data, dict):
            logger.error("Data must be a non-empty dictionary")
            return False
        
        try:
            with self.get_collection(collection_name) as collection:
                collection.document(document_id).set(data)
                logger.info(f"Document {document_id} set in {collection_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to set document {document_id}: {e}")
            return False
    
    def get_document(self, collection_name: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Get document from Firestore with error handling
        
        Args:
            collection_name: Firestore collection name
            document_id: Document ID
            
        Returns:
            Document data or None if error/not found
        """
        if not self.initialized:
            logger.error("Firebase not initialized")
            return None
        
        try:
            with self.get_collection(collection_name) as collection:
                doc = collection.document(document_id).get()
                if doc.exists:
                    return doc.to_dict()
                else:
                    logger.warning(f"Document {document_id} not found in {collection_name}")
                    return None
        except Exception as e:
            logger.error(f"Failed to get document {document_id}: {e}")
            return None
    
    def update_document(self, collection_name: str, document_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update document with merge strategy (preserves existing fields)
        
        Args:
            collection_name: Firestore collection name
            document_id: Document ID
            updates: