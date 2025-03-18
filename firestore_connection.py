import os
import json
from typing import Optional
from firebase_admin import credentials, firestore, initialize_app

class FirestoreConnection:
    """
    A class to manage the connection to Firestore database.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirestoreConnection, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize the Firestore connection."""
        try:
            # Check if we're already initialized
            self.db = firestore.client()
            print("Existing Firestore connection found.")
        except:
            # Initialize with service account credentials
            cred_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            
            if not cred_path:
                # For development, look for credentials in the current directory
                if os.path.exists('firebase-credentials.json'):
                    cred_path = 'firebase-credentials.json'
                else:
                    raise ValueError(
                        "Firebase credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS "
                        "environment variable or place firebase-credentials.json in the current directory."
                    )
            
            cred = credentials.Certificate(cred_path)
            initialize_app(cred)
            self.db = firestore.client()
            print("Initialized new Firestore connection.")
    
    def get_db(self):
        """Return the Firestore client instance."""
        return self.db
    
    def get_collection(self, collection_name: str):
        """
        Get a reference to a Firestore collection.
        
        Args:
            collection_name: Name of the collection to retrieve
            
        Returns:
            A Firestore collection reference
        """
        return self.db.collection(collection_name)
    
    def get_document(self, collection_name: str, document_id: str):
        """
        Get a specific document from a collection.
        
        Args:
            collection_name: Name of the collection
            document_id: ID of the document to retrieve
            
        Returns:
            Document data as a dictionary or None if not found
        """
        doc_ref = self.db.collection(collection_name).document(document_id)
        doc = doc_ref.get()
        
        if doc.exists:
            return doc.to_dict()
        else:
            return None
    
    def query_collection(self, collection_name: str, field: str, operator: str, value: any):
        """
        Query a collection with a simple filter.
        
        Args:
            collection_name: Name of the collection to query
            field: Field to filter on
            operator: Comparison operator ('==', '>', '<', '>=', '<=', 'array_contains')
            value: Value to compare against
            
        Returns:
            List of document dictionaries matching the query
        """
        docs = self.db.collection(collection_name).where(field, operator, value).stream()
        return [doc.to_dict() for doc in docs]

# Create a simple way to access the connection
def get_firestore():
    """Get the singleton Firestore connection instance."""
    return FirestoreConnection().get_db()

# Example usage
if __name__ == "__main__":
    try:
        # Get Firestore connection
        db = get_firestore()
        print("Successfully connected to Firestore.")
        
        # Test connection by listing collections
        collections = db.collections()
        print("Available collections:")
        for collection in collections:
            print(f" - {collection.id}")
    except Exception as e:
        print(f"Error connecting to Firestore: {e}")
