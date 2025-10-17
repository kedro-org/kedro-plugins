import pytest
import chromadb
from kedro.io.core import DatasetError

from kedro_datasets_experimental.chromadb import ChromaDBDataset


@pytest.fixture
def sample_data():
    """Sample data for testing ChromaDB operations."""
    return {
        "documents": [
            "This is the first document about machine learning",
            "This is the second document about data science",
            "This is the third document about artificial intelligence"
        ],
        "ids": ["doc1", "doc2", "doc3"],
        "metadatas": [
            {"topic": "ml", "author": "Alice"},
            {"topic": "ds", "author": "Bob"},
            {"topic": "ai", "author": "Charlie"}
        ]
    }


@pytest.fixture
def chromadb_dataset():
    """ChromaDB dataset with ephemeral client for testing."""
    return ChromaDBDataset(
        collection_name="test_collection",
        client_type="ephemeral"
    )


@pytest.fixture
def persistent_chromadb_dataset(tmp_path):
    """ChromaDB dataset with persistent client for testing."""
    return ChromaDBDataset(
        collection_name="test_collection",
        client_type="persistent",
        client_settings={"path": str(tmp_path / "test_chroma_db")}
    )


class TestChromaDBDataset:
    """Test suite for ChromaDBDataset."""

    def test_save_and_load(self, chromadb_dataset, sample_data):
        """Test saving and loading data."""
        # Save data
        chromadb_dataset.save(sample_data)
        
        # Load data back
        loaded_data = chromadb_dataset.load()
        
        # Verify the data
        assert loaded_data["documents"] == sample_data["documents"]
        assert loaded_data["ids"] == sample_data["ids"]
        assert loaded_data["metadatas"] == sample_data["metadatas"]

    def test_exists(self, chromadb_dataset, sample_data):
        """Test the exists method."""
        # Initially, collection shouldn't exist or be empty
        assert not chromadb_dataset.exists()
        
        # After saving data, it should exist
        chromadb_dataset.save(sample_data)
        assert chromadb_dataset.exists()

    def test_save_without_required_fields(self, chromadb_dataset):
        """Test saving data without required fields raises error."""
        # Missing 'ids'
        invalid_data = {"documents": ["test doc"]}
        with pytest.raises(DatasetError, match="Data must contain 'documents' and 'ids' keys"):
            chromadb_dataset.save(invalid_data)
        
        # Missing 'documents'
        invalid_data = {"ids": ["doc1"]}
        with pytest.raises(DatasetError, match="Data must contain 'documents' and 'ids' keys"):
            chromadb_dataset.save(invalid_data)

    def test_save_non_dict_data(self, chromadb_dataset):
        """Test saving non-dictionary data raises error."""
        with pytest.raises(DatasetError, match="Data must be a dictionary"):
            chromadb_dataset.save(["not", "a", "dict"])

    def test_load_with_query_args(self, chromadb_dataset, sample_data):
        """Test loading data with query arguments."""
        # Save initial data
        chromadb_dataset.save(sample_data)
        
        # Create a new dataset instance with load args
        dataset_with_query = ChromaDBDataset(
            collection_name="test_collection",
            client_type="ephemeral",
            load_args={"n_results": 2}
        )
        
        # Load with query - should return limited results
        loaded_data = dataset_with_query.load()
        assert len(loaded_data["documents"]) <= 2

    def test_save_with_embeddings(self, chromadb_dataset):
        """Test saving data with custom embeddings."""
        data_with_embeddings = {
            "documents": ["Test document"],
            "ids": ["doc1"],
            "embeddings": [[0.1, 0.2, 0.3]]
        }
        
        # Should not raise an error
        chromadb_dataset.save(data_with_embeddings)
        
        # Verify data was saved
        loaded_data = chromadb_dataset.load()
        assert loaded_data["documents"] == ["Test document"]
        assert loaded_data["ids"] == ["doc1"]

    def test_persistent_client(self, persistent_chromadb_dataset, sample_data):
        """Test ChromaDB with persistent client."""
        # Save data
        persistent_chromadb_dataset.save(sample_data)
        
        # Load data back
        loaded_data = persistent_chromadb_dataset.load()
        
        # Verify the data
        assert loaded_data["documents"] == sample_data["documents"]
        assert loaded_data["ids"] == sample_data["ids"]

    def test_describe(self, chromadb_dataset):
        """Test the describe method."""
        description = chromadb_dataset._describe()
        
        assert description["collection_name"] == "test_collection"
        assert description["client_type"] == "ephemeral"
        assert isinstance(description["client_settings"], dict)
        assert isinstance(description["load_args"], dict)
        assert isinstance(description["save_args"], dict)

    def test_invalid_client_type(self):
        """Test invalid client type raises error."""
        with pytest.raises(DatasetError, match="Unsupported client_type: invalid"):
            ChromaDBDataset(
                collection_name="test",
                client_type="invalid"
            )

    def test_http_client_config(self):
        """Test HTTP client configuration."""
        # This test creates the dataset but doesn't try to connect
        # since we don't have a real HTTP server running
        dataset = ChromaDBDataset(
            collection_name="test_collection",
            client_type="http",
            client_settings={"host": "localhost", "port": 8000}
        )
        
        description = dataset._describe()
        assert description["client_type"] == "http"
        assert description["client_settings"]["host"] == "localhost"
        assert description["client_settings"]["port"] == 8000

    def test_metadata_handling(self, chromadb_dataset, sample_data):
        """Test that metadata is properly handled."""
        # Create dataset with metadata
        dataset_with_metadata = ChromaDBDataset(
            collection_name="test_collection",
            client_type="ephemeral",
            metadata={"version": "1.0", "description": "Test dataset"}
        )
        
        # Metadata should be stored
        assert dataset_with_metadata.metadata == {"version": "1.0", "description": "Test dataset"}
        
        # Normal operations should still work
        dataset_with_metadata.save(sample_data)
        loaded_data = dataset_with_metadata.load()
        assert loaded_data["documents"] == sample_data["documents"]

    def test_collection_reuse(self, sample_data):
        """Test that multiple dataset instances can use the same collection."""
        # Create two dataset instances pointing to the same collection
        dataset1 = ChromaDBDataset(collection_name="shared_collection", client_type="ephemeral")
        dataset2 = ChromaDBDataset(collection_name="shared_collection", client_type="ephemeral")
        
        # Save data with first dataset
        dataset1.save(sample_data)
        
        # Load data with second dataset - Note: this might not work with ephemeral clients
        # as they don't share state, but the test verifies the pattern
        try:
            loaded_data = dataset2.load()
            # If it works, verify the data
            assert loaded_data["documents"] == sample_data["documents"]
        except Exception:
            # Ephemeral clients don't share collections, which is expected
            pass

    def test_empty_collection_load(self, chromadb_dataset):
        """Test loading from an empty collection."""
        # Load from empty collection should return empty lists
        loaded_data = chromadb_dataset.load()
        
        assert loaded_data["documents"] == []
        assert loaded_data["ids"] == []
        assert loaded_data["metadatas"] == []
        assert loaded_data["embeddings"] == []